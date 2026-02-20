"""
QuickBooks Online → Supabase Sync
Runs every 6 hours via Railway cron.
Syncs invoices, bills, and estimates from QBO to Supabase.
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime, timezone
from supabase import create_client

# ═══════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════

QBO_CLIENT_ID = os.environ.get("QBO_CLIENT_ID")
QBO_CLIENT_SECRET = os.environ.get("QBO_CLIENT_SECRET")
QBO_REALM_ID = os.environ.get("QBO_REALM_ID")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Initial refresh token from env (used only on first run; after that, Supabase stores it)
QBO_REFRESH_TOKEN_ENV = os.environ.get("QBO_REFRESH_TOKEN", "")

QBO_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QBO_API_BASE = f"https://quickbooks.api.intuit.com/v3/company/{QBO_REALM_ID}"
QBO_MINOR_VERSION = 75
QBO_PAGE_SIZE = 1000  # Max allowed by QBO API
MAX_RETRIES = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# SUPABASE CLIENT
# ═══════════════════════════════════════════════════════════════════════

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ═══════════════════════════════════════════════════════════════════════
# SYNC STATE HELPERS
# ═══════════════════════════════════════════════════════════════════════

def get_sync_state(key):
    """Get a value from qbo_sync_state table."""
    result = supabase.table("qbo_sync_state").select("value").eq("key", key).execute()
    if result.data and len(result.data) > 0:
        return result.data[0]["value"]
    return None


def set_sync_state(key, value):
    """Set a value in qbo_sync_state table."""
    supabase.table("qbo_sync_state").upsert({
        "key": key,
        "value": value,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).execute()


def get_refresh_token():
    """Get the current refresh token. Prefer Supabase, fall back to env var."""
    token = get_sync_state("refresh_token")
    if token and token.strip():
        return token
    return QBO_REFRESH_TOKEN_ENV


# ═══════════════════════════════════════════════════════════════════════
# OAUTH TOKEN REFRESH
# ═══════════════════════════════════════════════════════════════════════

def refresh_access_token():
    """
    Exchange refresh token for a new access token.
    QBO rotates refresh tokens on each use, so we must store the new one.
    Returns the new access token.
    """
    refresh_token = get_refresh_token()
    if not refresh_token:
        raise Exception("No refresh token available. Run OAuth authorization first.")

    logger.info("Refreshing QBO access token...")

    response = requests.post(
        QBO_TOKEN_URL,
        auth=(QBO_CLIENT_ID, QBO_CLIENT_SECRET),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
    )

    if response.status_code != 200:
        raise Exception(f"Token refresh failed: {response.status_code} — {response.text}")

    data = response.json()
    access_token = data["access_token"]
    new_refresh_token = data["refresh_token"]

    # Store the new refresh token in Supabase
    set_sync_state("refresh_token", new_refresh_token)
    logger.info("Access token refreshed. New refresh token stored.")

    return access_token


# ═══════════════════════════════════════════════════════════════════════
# QBO API QUERY
# ═══════════════════════════════════════════════════════════════════════

def qbo_query(access_token, query_string):
    """
    Execute a QBO API query and return all results (handles pagination).
    """
    all_results = []
    start_position = 1

    while True:
        paginated_query = f"{query_string} STARTPOSITION {start_position} MAXRESULTS {QBO_PAGE_SIZE}"
        url = f"{QBO_API_BASE}/query?minorversion={QBO_MINOR_VERSION}"

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    url,
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Accept": "application/json",
                        "Content-Type": "application/text",
                    },
                    params={"query": paginated_query},
                )

                if response.status_code == 401:
                    raise Exception("Access token expired or invalid")

                if response.status_code == 429:
                    wait = (attempt + 1) * 30
                    logger.warning(f"Rate limited. Waiting {wait}s (attempt {attempt + 1})")
                    time.sleep(wait)
                    continue

                if response.status_code != 200:
                    raise Exception(f"QBO API error {response.status_code}: {response.text[:300]}")

                data = response.json()
                query_response = data.get("QueryResponse", {})

                # Extract entity name from response (Invoice, Bill, Estimate, etc.)
                entity_key = None
                for key in query_response:
                    if key not in ("startPosition", "maxResults", "totalCount"):
                        entity_key = key
                        break

                if not entity_key or entity_key not in query_response:
                    return all_results  # No more results

                entities = query_response[entity_key]
                all_results.extend(entities)

                # Check if there are more pages
                if len(entities) < QBO_PAGE_SIZE:
                    return all_results

                start_position += QBO_PAGE_SIZE
                time.sleep(0.5)  # Be nice to the API
                break

            except Exception as e:
                if "expired" in str(e).lower() or "401" in str(e):
                    raise  # Don't retry auth errors
                if attempt == MAX_RETRIES - 1:
                    raise
                wait = (attempt + 1) * 10
                logger.warning(f"Query error, retrying in {wait}s: {e}")
                time.sleep(wait)

    return all_results


# ═══════════════════════════════════════════════════════════════════════
# DATA TRANSFORMERS
# ═══════════════════════════════════════════════════════════════════════

def safe_get(obj, *keys, default=None):
    """Safely navigate nested dicts."""
    current = obj
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return default
        if current is None:
            return default
    return current


def extract_address(addr_obj):
    """Extract address fields from a QBO address object."""
    if not addr_obj:
        return {}
    return {
        "line1": safe_get(addr_obj, "Line1", default=""),
        "city": safe_get(addr_obj, "City", default=""),
        "state": safe_get(addr_obj, "CountrySubDivisionCode", default=""),
        "postal_code": safe_get(addr_obj, "PostalCode", default=""),
        "country": safe_get(addr_obj, "Country", default=""),
    }


def extract_line_items(lines):
    """Extract line items from QBO Line array, filtering out subtotals."""
    if not lines:
        return []

    items = []
    for line in lines:
        detail_type = line.get("DetailType", "")

        # Skip subtotal lines
        if detail_type == "SubTotalLineDetail":
            continue

        item = {
            "id": line.get("Id"),
            "line_num": line.get("LineNum"),
            "amount": line.get("Amount"),
            "description": line.get("Description", ""),
            "detail_type": detail_type,
        }

        # Sales item details (invoices, estimates)
        if detail_type == "SalesItemLineDetail":
            detail = line.get("SalesItemLineDetail", {})
            item["item_id"] = safe_get(detail, "ItemRef", "value")
            item["item_name"] = safe_get(detail, "ItemRef", "name")
            item["qty"] = detail.get("Qty")
            item["unit_price"] = detail.get("UnitPrice")
            item["tax_code_id"] = safe_get(detail, "TaxCodeRef", "value")
            item["service_date"] = detail.get("ServiceDate")
            item["discount_rate"] = detail.get("DiscountRate")
            item["discount_amt"] = detail.get("DiscountAmt")

        # Item-based expense details (bills)
        elif detail_type == "ItemBasedExpenseLineDetail":
            detail = line.get("ItemBasedExpenseLineDetail", {})
            item["item_id"] = safe_get(detail, "ItemRef", "value")
            item["item_name"] = safe_get(detail, "ItemRef", "name")
            item["qty"] = detail.get("Qty")
            item["unit_price"] = detail.get("UnitPrice")
            item["customer_id"] = safe_get(detail, "CustomerRef", "value")
            item["customer_name"] = safe_get(detail, "CustomerRef", "name")
            item["billable_status"] = detail.get("BillableStatus")
            item["tax_code_id"] = safe_get(detail, "TaxCodeRef", "value")

        # Account-based expense details (bills)
        elif detail_type == "AccountBasedExpenseLineDetail":
            detail = line.get("AccountBasedExpenseLineDetail", {})
            item["account_id"] = safe_get(detail, "AccountRef", "value")
            item["account_name"] = safe_get(detail, "AccountRef", "name")
            item["customer_id"] = safe_get(detail, "CustomerRef", "value")
            item["customer_name"] = safe_get(detail, "CustomerRef", "name")
            item["billable_status"] = detail.get("BillableStatus")
            item["tax_code_id"] = safe_get(detail, "TaxCodeRef", "value")

        # Discount line detail
        elif detail_type == "DiscountLineDetail":
            detail = line.get("DiscountLineDetail", {})
            item["discount_percent"] = detail.get("DiscountPercent")
            item["percent_based"] = detail.get("PercentBased")

        items.append(item)

    return items


def extract_linked_txns(linked):
    """Extract linked transactions."""
    if not linked:
        return []
    return [
        {
            "txn_id": lt.get("TxnId"),
            "txn_type": lt.get("TxnType"),
            "txn_line_id": lt.get("TxnLineId"),
        }
        for lt in linked
    ]


def transform_invoice(inv):
    """Transform a QBO Invoice object to a Supabase row."""
    bill_addr = extract_address(inv.get("BillAddr"))
    ship_addr = extract_address(inv.get("ShipAddr"))

    # ── FIX: QBO only returns HomeTotalAmt / HomeBalance for multi-currency
    # transactions. For USD (home currency) invoices these fields are absent,
    # which results in NULLs in Supabase. Queries like "home_balance > 0"
    # then silently exclude every USD invoice (NULL > 0 → NULL → filtered out).
    # Solution: fall back to the transaction-currency values, which are
    # identical to home-currency values when the invoice IS in home currency.
    home_total = inv.get("HomeTotalAmt")
    if home_total is None:
        home_total = inv.get("TotalAmt")

    home_balance = inv.get("HomeBalance")
    if home_balance is None:
        home_balance = inv.get("Balance")

    return {
        "qbo_id": str(inv["Id"]),
        "doc_number": inv.get("DocNumber"),
        "txn_date": inv.get("TxnDate"),
        "due_date": inv.get("DueDate"),
        "ship_date": inv.get("ShipDate"),
        "total_amt": inv.get("TotalAmt"),
        "balance": inv.get("Balance"),
        "deposit": inv.get("Deposit"),
        "home_total_amt": home_total,       # ← was inv.get("HomeTotalAmt")
        "home_balance": home_balance,        # ← was inv.get("HomeBalance")
        "customer_id": safe_get(inv, "CustomerRef", "value"),
        "customer_name": safe_get(inv, "CustomerRef", "name"),
        "bill_addr_line1": bill_addr.get("line1"),
        "bill_addr_city": bill_addr.get("city"),
        "bill_addr_state": bill_addr.get("state"),
        "bill_addr_postal_code": bill_addr.get("postal_code"),
        "bill_addr_country": bill_addr.get("country"),
        "ship_addr_line1": ship_addr.get("line1"),
        "ship_addr_city": ship_addr.get("city"),
        "ship_addr_state": ship_addr.get("state"),
        "ship_addr_postal_code": ship_addr.get("postal_code"),
        "ship_addr_country": ship_addr.get("country"),
        "bill_email": safe_get(inv, "BillEmail", "Address"),
        "email_status": inv.get("EmailStatus"),
        "print_status": inv.get("PrintStatus"),
        "txn_status": inv.get("TxnStatus"),
        "apply_tax_after_discount": inv.get("ApplyTaxAfterDiscount"),
        "allow_online_payment": inv.get("AllowOnlinePayment"),
        "allow_online_credit_card_payment": inv.get("AllowOnlineCreditCardPayment"),
        "allow_online_ach_payment": inv.get("AllowOnlineACHPayment"),
        "sales_term_id": safe_get(inv, "SalesTermRef", "value"),
        "sales_term_name": safe_get(inv, "SalesTermRef", "name"),
        "payment_method_id": safe_get(inv, "PaymentMethodRef", "value"),
        "payment_method_name": safe_get(inv, "PaymentMethodRef", "name"),
        "currency_code": safe_get(inv, "CurrencyRef", "value"),
        "exchange_rate": inv.get("ExchangeRate"),
        "private_note": inv.get("PrivateNote"),
        "customer_memo": safe_get(inv, "CustomerMemo", "value"),
        "tracking_num": inv.get("TrackingNum"),
        "ship_method_name": safe_get(inv, "ShipMethodRef", "name"),
        "linked_txns": json.dumps(extract_linked_txns(inv.get("LinkedTxn"))),
        "line_items": json.dumps(extract_line_items(inv.get("Line"))),
        "created_time": safe_get(inv, "MetaData", "CreateTime"),
        "last_updated_time": safe_get(inv, "MetaData", "LastUpdatedTime"),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def transform_bill(bill):
    """Transform a QBO Bill object to a Supabase row."""

    # ── FIX: Same home-currency NULL issue as invoices (see transform_invoice)
    home_total = bill.get("HomeTotalAmt")
    if home_total is None:
        home_total = bill.get("TotalAmt")

    home_balance = bill.get("HomeBalance")
    if home_balance is None:
        home_balance = bill.get("Balance")

    return {
        "qbo_id": str(bill["Id"]),
        "doc_number": bill.get("DocNumber"),
        "txn_date": bill.get("TxnDate"),
        "due_date": bill.get("DueDate"),
        "total_amt": bill.get("TotalAmt"),
        "balance": bill.get("Balance"),
        "home_total_amt": home_total,        # ← was bill.get("HomeTotalAmt")
        "home_balance": home_balance,         # ← was bill.get("HomeBalance")
        "vendor_id": safe_get(bill, "VendorRef", "value"),
        "vendor_name": safe_get(bill, "VendorRef", "name"),
        "ap_account_id": safe_get(bill, "APAccountRef", "value"),
        "ap_account_name": safe_get(bill, "APAccountRef", "name"),
        "currency_code": safe_get(bill, "CurrencyRef", "value"),
        "exchange_rate": bill.get("ExchangeRate"),
        "private_note": bill.get("PrivateNote"),
        "linked_txns": json.dumps(extract_linked_txns(bill.get("LinkedTxn"))),
        "line_items": json.dumps(extract_line_items(bill.get("Line"))),
        "created_time": safe_get(bill, "MetaData", "CreateTime"),
        "last_updated_time": safe_get(bill, "MetaData", "LastUpdatedTime"),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def transform_estimate(est):
    """Transform a QBO Estimate object to a Supabase row."""
    bill_addr = extract_address(est.get("BillAddr"))
    ship_addr = extract_address(est.get("ShipAddr"))

    # ── FIX: Same home-currency NULL issue (see transform_invoice)
    home_total = est.get("HomeTotalAmt")
    if home_total is None:
        home_total = est.get("TotalAmt")

    return {
        "qbo_id": str(est["Id"]),
        "doc_number": est.get("DocNumber"),
        "txn_date": est.get("TxnDate"),
        "expiration_date": est.get("ExpirationDate"),
        "total_amt": est.get("TotalAmt"),
        "home_total_amt": home_total,        # ← was est.get("HomeTotalAmt")
        "customer_id": safe_get(est, "CustomerRef", "value"),
        "customer_name": safe_get(est, "CustomerRef", "name"),
        "bill_addr_line1": bill_addr.get("line1"),
        "bill_addr_city": bill_addr.get("city"),
        "bill_addr_state": bill_addr.get("state"),
        "bill_addr_postal_code": bill_addr.get("postal_code"),
        "bill_addr_country": bill_addr.get("country"),
        "ship_addr_line1": ship_addr.get("line1"),
        "ship_addr_city": ship_addr.get("city"),
        "ship_addr_state": ship_addr.get("state"),
        "ship_addr_postal_code": ship_addr.get("postal_code"),
        "ship_addr_country": ship_addr.get("country"),
        "bill_email": safe_get(est, "BillEmail", "Address"),
        "txn_status": est.get("TxnStatus"),
        "email_status": est.get("EmailStatus"),
        "print_status": est.get("PrintStatus"),
        "accepted_by": est.get("AcceptedBy"),
        "accepted_date": est.get("AcceptedDate"),
        "sales_term_id": safe_get(est, "SalesTermRef", "value"),
        "sales_term_name": safe_get(est, "SalesTermRef", "name"),
        "currency_code": safe_get(est, "CurrencyRef", "value"),
        "exchange_rate": est.get("ExchangeRate"),
        "private_note": est.get("PrivateNote"),
        "customer_memo": safe_get(est, "CustomerMemo", "value"),
        "linked_txns": json.dumps(extract_linked_txns(est.get("LinkedTxn"))),
        "line_items": json.dumps(extract_line_items(est.get("Line"))),
        "created_time": safe_get(est, "MetaData", "CreateTime"),
        "last_updated_time": safe_get(est, "MetaData", "LastUpdatedTime"),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


# ═══════════════════════════════════════════════════════════════════════
# UPSERT TO SUPABASE
# ═══════════════════════════════════════════════════════════════════════

def upsert_batch(table, rows, batch_size=50):
    """Upsert rows to a Supabase table in batches."""
    if not rows:
        return 0

    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        for attempt in range(MAX_RETRIES):
            try:
                supabase.table(table).upsert(batch).execute()
                total += len(batch)
                break
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Failed to upsert batch to {table}: {e}")
                    raise
                wait = (attempt + 1) * 5
                logger.warning(f"Upsert error on {table}, retrying in {wait}s: {e}")
                time.sleep(wait)

    return total


# ═══════════════════════════════════════════════════════════════════════
# SYNC ENTITY
# ═══════════════════════════════════════════════════════════════════════

def sync_entity(access_token, entity_name, table_name, transformer, state_key):
    """Sync a single QBO entity type to Supabase."""
    logger.info(f"Syncing {entity_name}...")

    query = f"SELECT * FROM {entity_name}"
    entities = qbo_query(access_token, query)
    logger.info(f"  Fetched {len(entities)} {entity_name.lower()}s from QBO")

    if not entities:
        set_sync_state(state_key, datetime.now(timezone.utc).isoformat())
        return 0

    rows = [transformer(e) for e in entities]
    count = upsert_batch(table_name, rows)
    set_sync_state(state_key, datetime.now(timezone.utc).isoformat())
    logger.info(f"  Upserted {count} {entity_name.lower()}s to Supabase")

    return count


# ═══════════════════════════════════════════════════════════════════════
# MAIN SYNC
# ═══════════════════════════════════════════════════════════════════════

def run_sync():
    """Main sync function. Refreshes token and syncs all entities."""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("QBO SYNC STARTING")
    logger.info("=" * 60)

    # Validate config
    missing = []
    for var in ["QBO_CLIENT_ID", "QBO_CLIENT_SECRET", "QBO_REALM_ID", "SUPABASE_URL", "SUPABASE_KEY"]:
        if not os.environ.get(var):
            missing.append(var)
    if missing:
        logger.error(f"Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

    try:
        # Refresh access token (also rotates and stores new refresh token)
        access_token = refresh_access_token()

        # Sync each entity
        totals = {}
        totals["invoices"] = sync_entity(
            access_token, "Invoice", "qbo_invoices", transform_invoice, "last_sync_invoices"
        )
        totals["bills"] = sync_entity(
            access_token, "Bill", "qbo_bills", transform_bill, "last_sync_bills"
        )
        totals["estimates"] = sync_entity(
            access_token, "Estimate", "qbo_estimates", transform_estimate, "last_sync_estimates"
        )

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"QBO SYNC COMPLETE in {elapsed:.1f}s")
        logger.info(f"  Invoices: {totals['invoices']}")
        logger.info(f"  Bills: {totals['bills']}")
        logger.info(f"  Estimates: {totals['estimates']}")
        logger.info("=" * 60)

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"QBO SYNC FAILED after {elapsed:.1f}s: {e}")
        raise


# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run_sync()

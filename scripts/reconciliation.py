#!/usr/bin/env python3
"""
Reconciliation Script - Phase 3 Data Validation
Compares the source CSV against the destination database and outputs
a report: total source records, successfully migrated, failed/skipped,
and total financial value.
"""

import csv
import sys
from decimal import Decimal, InvalidOperation
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

CSV_PATH = "data/transactions.csv"


class DataReconciliator:

    def __init__(self, host="localhost", port=5432, user="gig_user",
                 password="gig_password", database="gig_db"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = psycopg2.connect(
            host=self.host, port=self.port, user=self.user,
            password=self.password, database=self.database
        )
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def parse_csv(self):
        """Read CSV and classify each row as valid, duplicate, or malformed."""
        all_rows = []
        try:
            with open(CSV_PATH, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    all_rows.append(row)
        except FileNotFoundError:
            print(f"  WARNING: CSV not found at {CSV_PATH} — skipping source analysis")
            return [], [], []

        valid = {}      # transaction_id → row (highest amount wins)
        malformed = []  # rows that can't be parsed

        for row in all_rows:
            tid = row.get("transaction_id", "").strip()
            aid = row.get("account_id", "").strip()
            amt = row.get("amount", "").strip()

            # Detect malformed rows
            if not tid or not aid:
                malformed.append({"reason": "missing transaction_id or account_id", "row": row})
                continue
            try:
                amount = Decimal(amt)
            except InvalidOperation:
                malformed.append({"reason": f"invalid amount '{amt}'", "row": row})
                continue

            # Duplicate resolution: keep highest amount
            if tid in valid:
                if amount > valid[tid]["_amount"]:
                    valid[tid] = {**row, "_amount": amount}
            else:
                valid[tid] = {**row, "_amount": amount}

        return all_rows, list(valid.values()), malformed

    def query_db(self):
        self.cursor.execute("""
            SELECT transaction_id, amount, currency
            FROM gig.transactions_source
            ORDER BY transaction_id
        """)
        return self.cursor.fetchall()

    def run(self):
        print("=" * 55)
        print("  Phase 3 — Migration Reconciliation Report")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 55)
        print()

        try:
            self.connect()

            all_rows, valid_rows, malformed_rows = self.parse_csv()
            db_rows = self.query_db()

            total_source = len(all_rows)
            total_malformed = len(malformed_rows)
            # Rows that are valid in CSV but lost to duplicate resolution
            total_valid_unique = len(valid_rows)
            total_duplicate_discarded = total_source - total_malformed - total_valid_unique

            total_in_db = len(db_rows)
            db_ids = {r["transaction_id"] for r in db_rows}
            valid_ids = {r["transaction_id"] for r in valid_rows}

            not_migrated = valid_ids - db_ids       # should have been migrated but aren't
            extra_in_db = db_ids - valid_ids        # in DB but not in valid CSV

            # Financial value: sum of amounts in DB
            total_value_by_currency = {}
            for r in db_rows:
                cur = r["currency"]
                total_value_by_currency[cur] = total_value_by_currency.get(cur, Decimal("0")) + r["amount"]

            # Print source analysis
            print("SOURCE (CSV)")
            print(f"  Total rows              : {total_source}")
            print(f"  Malformed / skipped     : {total_malformed}")
            for m in malformed_rows:
                tid = m['row'].get('transaction_id', '(empty)') or '(empty)'
                print(f"    - {tid}: {m['reason']}")
            print(f"  Duplicate rows resolved : {total_duplicate_discarded}")
            print(f"  Unique valid records    : {total_valid_unique}")
            print()

            print("DESTINATION (Database)")
            print(f"  Records in DB           : {total_in_db}")
            print(f"  Not migrated            : {len(not_migrated)}")
            if not_migrated:
                for tid in sorted(not_migrated):
                    print(f"    - {tid}")
            print(f"  Unexpected in DB        : {len(extra_in_db)}")
            print()

            print("FINANCIAL VALUE (persisted)")
            for cur, total in sorted(total_value_by_currency.items()):
                print(f"  {cur}: {total:,.2f}")
            print()

            # Summary
            successfully_migrated = len(db_ids & valid_ids)
            failed_skipped = total_malformed + len(not_migrated)

            print("=" * 55)
            print("SUMMARY")
            print(f"  Total Source Records    : {total_source}")
            print(f"  Successfully Migrated   : {successfully_migrated}")
            print(f"  Failed / Skipped        : {failed_skipped}  "
                  f"({total_malformed} malformed, {len(not_migrated)} not in DB)")
            print(f"  Duplicate rows resolved : {total_duplicate_discarded} (kept highest amount)")
            total_all = sum(total_value_by_currency.values())
            print(f"  Total Financial Value   : {total_all:,.2f} (mixed currencies)")
            print()
            overall_pass = (successfully_migrated == total_valid_unique and
                            len(not_migrated) == 0 and len(extra_in_db) == 0)
            print(f"  Overall result          : {'PASS' if overall_pass else 'FAIL'}")
            print("=" * 55)

            if not overall_pass:
                sys.exit(1)

        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            self.disconnect()


if __name__ == "__main__":
    DataReconciliator().run()

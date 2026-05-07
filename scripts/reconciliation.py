#!/usr/bin/env python3
"""
Reconciliation Script - Phase 3 Data Validation
Validates data integrity between source and target tables
"""

import sys
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor


class DataReconciliator:

    def __init__(self, host='localhost', port=5432, user='gig_user', password='gig_password', database='gig_db'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None
        self.results = {}

    def connect(self):
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        print(f"Connected to {self.database} at {self.host}:{self.port}")

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def validate_record_count(self):
        self.cursor.execute("SELECT COUNT(*) AS cnt FROM gig.transactions_source")
        source_count = self.cursor.fetchone()['cnt']

        self.cursor.execute("SELECT COUNT(*) AS cnt FROM gig.transactions_target")
        target_count = self.cursor.fetchone()['cnt']

        match = source_count == target_count
        self.results['record_count'] = {
            'source': source_count,
            'target': target_count,
            'match': match
        }

        status = "PASS" if match else "FAIL"
        print(f"  [{status}] Record count — source: {source_count}, target: {target_count}")
        return match

    def validate_data_integrity(self):
        issues = []

        self.cursor.execute("""
            SELECT COUNT(*) AS cnt FROM gig.transactions_source
            WHERE transaction_id IS NULL OR user_id IS NULL OR amount IS NULL
               OR currency IS NULL OR transaction_date IS NULL OR status IS NULL
        """)
        source_nulls = self.cursor.fetchone()['cnt']

        self.cursor.execute("""
            SELECT COUNT(*) AS cnt FROM gig.transactions_target
            WHERE transaction_id IS NULL OR user_id IS NULL OR amount IS NULL
               OR currency IS NULL OR transaction_date IS NULL OR status IS NULL
        """)
        target_nulls = self.cursor.fetchone()['cnt']

        if source_nulls > 0:
            issues.append(f"source has {source_nulls} rows with NULL required fields")
        if target_nulls > 0:
            issues.append(f"target has {target_nulls} rows with NULL required fields")

        self.cursor.execute("SELECT COUNT(*) AS cnt FROM gig.transactions_source WHERE amount < 0")
        neg_amounts = self.cursor.fetchone()['cnt']
        if neg_amounts > 0:
            issues.append(f"source has {neg_amounts} rows with negative amount")

        self.results['data_integrity'] = {'issues': issues, 'pass': len(issues) == 0}
        status = "PASS" if not issues else "FAIL"
        print(f"  [{status}] Data integrity — {len(issues)} issue(s)")
        for issue in issues:
            print(f"           * {issue}")
        return len(issues) == 0

    def validate_reconciliation(self):
        self.cursor.execute("""
            SELECT s.transaction_id,
                   s.amount AS source_amount, t.amount AS target_amount,
                   s.status  AS source_status,  t.status  AS target_status
            FROM gig.transactions_source s
            JOIN gig.transactions_target t USING (transaction_id)
            WHERE s.amount <> t.amount OR s.status <> t.status
        """)
        mismatches = self.cursor.fetchall()

        self.cursor.execute("""
            SELECT transaction_id FROM gig.transactions_source
            WHERE transaction_id NOT IN (SELECT transaction_id FROM gig.transactions_target)
        """)
        missing_in_target = [r['transaction_id'] for r in self.cursor.fetchall()]

        self.cursor.execute("""
            SELECT transaction_id FROM gig.transactions_target
            WHERE transaction_id NOT IN (SELECT transaction_id FROM gig.transactions_source)
        """)
        missing_in_source = [r['transaction_id'] for r in self.cursor.fetchall()]

        self.results['reconciliation'] = {
            'mismatches': len(mismatches),
            'missing_in_target': missing_in_target,
            'missing_in_source': missing_in_source,
            'pass': len(mismatches) == 0 and not missing_in_target and not missing_in_source
        }

        passed = self.results['reconciliation']['pass']
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] Reconciliation — {len(mismatches)} amount/status mismatch(es), "
              f"{len(missing_in_target)} missing in target, {len(missing_in_source)} missing in source")
        for m in mismatches:
            print(f"           * {m['transaction_id']}: amount {m['source_amount']} vs {m['target_amount']}, "
                  f"status {m['source_status']} vs {m['target_status']}")
        return passed

    def generate_report(self):
        passed = sum(1 for v in self.results.values()
                     if isinstance(v, dict) and v.get('pass', v.get('match', False)))
        total = len(self.results)

        print()
        print("=" * 50)
        print("RECONCILIATION REPORT")
        print("=" * 50)
        print(f"  Source records : {self.results.get('record_count', {}).get('source', 'N/A')}")
        print(f"  Target records : {self.results.get('record_count', {}).get('target', 'N/A')}")
        print(f"  Checks passed  : {passed}/{total}")
        overall = passed == total
        print(f"  Overall result : {'PASS' if overall else 'FAIL'}")
        print("=" * 50)
        return overall

    def run(self):
        print("Starting Phase 3 Reconciliation...")
        print(f"Timestamp: {datetime.now()}")
        print()

        try:
            self.connect()
            print("Running checks:")
            self.validate_record_count()
            self.validate_data_integrity()
            self.validate_reconciliation()
            overall = self.generate_report()
            if not overall:
                sys.exit(1)
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            self.disconnect()


if __name__ == "__main__":
    reconciliator = DataReconciliator()
    reconciliator.run()

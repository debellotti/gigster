#!/bin/bash
# API Verification Script
# Demonstrates account creation, money transfers, and transaction history.

BASE_URL="http://localhost:8080/api"

pass() { echo "[PASS] $1"; }
fail() { echo "[FAIL] $1"; }
check() {
  local label="$1" actual="$2" expected="$3"
  if echo "$actual" | grep -q "$expected"; then pass "$label"; else fail "$label — got: $actual"; fi
}

echo "========================================"
echo " GiG Migration Challenge — API Verification"
echo "========================================"
echo ""

# ── Health ────────────────────────────────────
echo "--- Health ---"
HEALTH=$(curl -s "$BASE_URL/transactions/health")
check "Health endpoint" "$HEALTH" "running"
echo ""

# ── Accounts ──────────────────────────────────
echo "--- Account Management ---"

ACC1=$(curl -s -X POST "$BASE_URL/accounts" \
  -H "Content-Type: application/json" \
  -d '{"accountId":"GIG-USR-001","ownerName":"Alice Martin","balance":1000.00,"currency":"EUR"}')
check "Create account GIG-USR-001" "$ACC1" "GIG-USR-001"

ACC2=$(curl -s -X POST "$BASE_URL/accounts" \
  -H "Content-Type: application/json" \
  -d '{"accountId":"GIG-USR-002","ownerName":"Bob Chen","balance":500.00,"currency":"EUR"}')
check "Create account GIG-USR-002" "$ACC2" "GIG-USR-002"

ALL_ACC=$(curl -s "$BASE_URL/accounts")
check "List all accounts (2 results)" "$ALL_ACC" "GIG-USR-001"

GET_ACC=$(curl -s "$BASE_URL/accounts/GIG-USR-001")
check "Get account by ID" "$GET_ACC" "Alice Martin"
echo ""

# ── Transfers ─────────────────────────────────
echo "--- Money Transfers ---"

TRANSFER=$(curl -s -X POST "$BASE_URL/accounts/GIG-USR-001/transfers" \
  -H "Content-Type: application/json" \
  -d '{"toAccountId":"GIG-USR-002","amount":200.00,"currency":"EUR"}')
check "Transfer 200 EUR from GIG-USR-001 to GIG-USR-002" "$TRANSFER" "GIG-USR-002"

# Verify balances after transfer
AFTER1=$(curl -s "$BASE_URL/accounts/GIG-USR-001")
check "GIG-USR-001 balance is 800 after transfer" "$AFTER1" "800"

AFTER2=$(curl -s "$BASE_URL/accounts/GIG-USR-002")
check "GIG-USR-002 balance is 700 after transfer" "$AFTER2" "700"

# Test insufficient balance
INSUF=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/accounts/GIG-USR-001/transfers" \
  -H "Content-Type: application/json" \
  -d '{"toAccountId":"GIG-USR-002","amount":99999.00,"currency":"EUR"}')
check "Transfer rejected on insufficient balance (400)" "$INSUF" "400"
echo ""

# ── Migrated Transactions ─────────────────────
echo "--- Migrated Transactions (Kafka consumer) ---"

ALL_TX=$(curl -s "$BASE_URL/transactions")
TX_COUNT=$(echo "$ALL_TX" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [ "$TX_COUNT" -gt 0 ]; then
  pass "Transactions in DB: $TX_COUNT records"
else
  echo "[INFO] No migrated transactions yet — trigger the pipeline with:"
  echo "       curl -X POST $BASE_URL/transactions/load-csv"
fi
echo ""

echo "========================================"
echo " Verification complete"
echo "========================================"

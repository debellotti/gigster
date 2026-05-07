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

# Create accounts (409 is acceptable — account already exists from a previous run)
ACC1=$(curl -s -X POST "$BASE_URL/accounts" \
  -H "Content-Type: application/json" \
  -d '{"accountId":"GIG-USR-001","ownerName":"Alice Martin","balance":1000.00,"currency":"EUR"}')
if echo "$ACC1" | grep -q "GIG-USR-001"; then
  pass "Create account GIG-USR-001"
elif echo "$ACC1" | grep -q "already exists"; then
  pass "Create account GIG-USR-001 (already exists)"
else
  fail "Create account GIG-USR-001 — got: $ACC1"
fi

ACC2=$(curl -s -X POST "$BASE_URL/accounts" \
  -H "Content-Type: application/json" \
  -d '{"accountId":"GIG-USR-002","ownerName":"Bob Chen","balance":500.00,"currency":"EUR"}')
if echo "$ACC2" | grep -q "GIG-USR-002"; then
  pass "Create account GIG-USR-002"
elif echo "$ACC2" | grep -q "already exists"; then
  pass "Create account GIG-USR-002 (already exists)"
else
  fail "Create account GIG-USR-002 — got: $ACC2"
fi

ALL_ACC=$(curl -s "$BASE_URL/accounts")
check "List all accounts includes GIG-USR-001" "$ALL_ACC" "GIG-USR-001"

GET_ACC=$(curl -s "$BASE_URL/accounts/GIG-USR-001")
check "Get account by ID returns Alice Martin" "$GET_ACC" "Alice Martin"
echo ""

# ── Transfers ─────────────────────────────────
echo "--- Money Transfers ---"

# Read current balance before transfer so the test is repeatable
BAL_BEFORE=$(curl -s "$BASE_URL/accounts/GIG-USR-001" | python3 -c "import json,sys; print(json.load(sys.stdin)['balance'])" 2>/dev/null || echo "0")
EXPECTED_AFTER=$(python3 -c "from decimal import Decimal; print(int(Decimal('$BAL_BEFORE') - Decimal('200')))")

TRANSFER=$(curl -s -X POST "$BASE_URL/accounts/GIG-USR-001/transfers" \
  -H "Content-Type: application/json" \
  -d '{"toAccountId":"GIG-USR-002","amount":200.00,"currency":"EUR"}')
check "Transfer 200 EUR from GIG-USR-001 to GIG-USR-002" "$TRANSFER" "GIG-USR-002"

AFTER1=$(curl -s "$BASE_URL/accounts/GIG-USR-001")
check "GIG-USR-001 balance decreased by 200 (now $EXPECTED_AFTER)" "$AFTER1" "$EXPECTED_AFTER"

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

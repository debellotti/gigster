#!/bin/bash

# API Verification Script - Phase 1 Testing
# Tests all Java Service API endpoints

BASE_URL="http://localhost:8080/api"
TRANSACTION_ID="TXN001"

echo "========================================"
echo "Gig Migration Challenge - API Verification"
echo "========================================"
echo ""

# Test 1: Health Check
echo "[TEST 1] Health Check Endpoint"
curl -s "$BASE_URL/transactions/health" | jq . || echo "FAILED"
echo ""

# Test 2: Get All Transactions
echo "[TEST 2] Get All Transactions"
curl -s "$BASE_URL/transactions" | jq . || echo "FAILED"
echo ""

# Test 3: Get Specific Transaction
echo "[TEST 3] Get Transaction by ID ($TRANSACTION_ID)"
curl -s "$BASE_URL/transactions/$TRANSACTION_ID" | jq . || echo "FAILED"
echo ""

# Test 4: Create Transaction
echo "[TEST 4] Create New Transaction"
curl -s -X POST "$BASE_URL/transactions" \
  -H "Content-Type: application/json" \
  -d '{
    "transactionId": "TXN999",
    "userId": "USR999",
    "amount": 99.99,
    "currency": "USD",
    "transactionDate": "2026-05-04T00:00:00",
    "status": "PENDING",
    "description": "Test transaction"
  }' | jq . || echo "FAILED"
echo ""

echo "========================================"
echo "Verification Complete"
echo "========================================"

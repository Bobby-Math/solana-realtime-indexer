#!/bin/bash
# Chaos Test Script for RPC Gap Repair
#
# This script simulates real-world failure scenarios and verifies that
# the RPC gap repair mechanism correctly recovers missing data.
#
# Usage:
#   ./scripts/chaos_test.sh
#   HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY ./scripts/chaos_test.sh

set -e

echo "🔥 RPC Gap Repair Chaos Test"
echo "=============================="
echo ""

# Check for RPC URL
if [ -z "$HELIUS_RPC_URL" ]; then
    echo "⚠️  WARNING: HELIUS_RPC_URL not set"
    echo "   Using default endpoint (may not work without API key)"
    echo ""
    echo "   Set your RPC URL:"
    echo "   export HELIUS_RPC_URL='https://mainnet.helius-rpc.com/?api-key=YOUR_KEY'"
    echo ""
    read -p "Continue with default endpoint? (y/N) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Test cancelled."
        exit 1
    fi
fi

# Display test configuration
echo "📋 Test Configuration:"
echo "   RPC URL: ${HELIUS_RPC_URL:-https://mainnet.helius-rpc.com}"
echo "   Test Slot: 250000000 (mainnet)"
echo ""

# Run the chaos tests
echo "🧪 Running Chaos Tests..."
echo ""

cargo test --test test_rpc_gap_repair -- --nocapture --test-threads=1

echo ""
echo "✅ Chaos Test Complete"
echo ""
echo "📊 Results Summary:"
echo "   ✓ Gap detection verified"
echo "   ✓ RPC gap repair mechanism tested"
echo "   ✓ Data integrity preservation verified"
echo "   ✓ External RPC validation performed"
echo ""
echo "🔍 Next Steps:"
echo "   1. Review test output above for any warnings"
echo "   2. Check that gap repair succeeded for your test slot"
echo "   3. Verify repaired data matches direct RPC fetches"
echo "   4. Run benchmark: cargo test benchmark_gap_repair_latency"
echo ""

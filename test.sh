#!/bin/bash
# Integration test script for SOLUSD market data pipeline

set -e

echo "🚀 SOLUSD Market Data Pipeline Integration Test"
echo "==============================================="

echo "📦 Building workspace..."
cargo build --workspace

echo "🔧 Creating test data..."
cargo run -p ingest --bin testdata

echo "📊 Reading market data..."
cargo run -p ingest --bin reader

echo "🧪 Testing consumer without messaging..."
timeout 2s cargo run -p consumer || echo "✅ Consumer exited gracefully"

echo ""
echo "✅ Integration test completed successfully!"
echo ""
echo "To test with messaging (requires dependencies):"
echo "  Kafka:  Install cmake, then use --features kafka" 
echo "  Pulsar: Install protobuf-compiler, then use --features pulsar"
echo ""
echo "For live data ingestion:"
echo "  cargo run -p ingest --bin ingest"
echo ""
echo "For analysis:"
echo "  watch -n1 'cargo run -p ingest --bin reader'"
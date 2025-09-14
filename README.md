# Gemini SOLUSD Low-Latency Ingestion and Trading Data Pipeline (Rust)

This workspace contains:
- `shared`: Shared-memory primitives (OrderBook depth 50, TopOfBook) using `memmap2` with lock-free volatile access.
- `ingest`: Connects to Gemini WebSockets (v2 L2 order book, v1 top-of-book + trades), writes to shared memory, and publishes trades to Kafka.
- `consumer`: Kafka consumer that stores trades into Postgres and enforces 7-day retention.

## Quick start

Environment variables:
- `OB_MMAP` (default `/dev/shm/solusd_order_book.mmap`)
- `TOB_MMAP` (default `/dev/shm/solusd_top_of_book.mmap`)
- `KAFKA_BROKERS` (default `localhost:9092`)
- `KAFKA_TOPIC` (default `gemini.trades`)
- `PULSAR_URL` (default `pulsar://localhost:6650`)
- `PG_DSN` (default `host=localhost user=postgres password=postgres dbname=trades`)

### Build

```bash
cargo build --workspace
```

### Run ingestion

**Without messaging (shared memory only):**
```bash
OB_MMAP=/dev/shm/solusd_order_book.mmap \
TOB_MMAP=/dev/shm/solusd_top_of_book.mmap \
cargo run -p ingest --bin ingest
```

**With Kafka messaging:**
```bash
OB_MMAP=/dev/shm/solusd_order_book.mmap \
TOB_MMAP=/dev/shm/solusd_top_of_book.mmap \
KAFKA_BROKERS=localhost:9092 \
KAFKA_TOPIC=gemini.trades \
cargo run -p ingest --bin ingest --features kafka
```

**With Pulsar messaging:**
```bash
OB_MMAP=/dev/shm/solusd_order_book.mmap \
TOB_MMAP=/dev/shm/solusd_top_of_book.mmap \
PULSAR_URL=pulsar://localhost:6650 \
KAFKA_TOPIC=gemini.trades \
cargo run -p ingest --bin ingest --features pulsar
```

### Run consumer (Postgres)

**Kafka consumer:**
```bash
PG_DSN="host=localhost user=postgres password=postgres dbname=trades" \
KAFKA_BROKERS=localhost:9092 \
KAFKA_TOPIC=gemini.trades \
cargo run -p consumer --features kafka
```

**Pulsar consumer:**
```bash
PG_DSN="host=localhost user=postgres password=postgres dbname=trades" \
PULSAR_URL=pulsar://localhost:6650 \
KAFKA_TOPIC=gemini.trades \
cargo run -p consumer --features pulsar
```

### Read market data (smoke test)

```bash
# Create test data
cargo run -p ingest --bin testdata

# Read and display current market data
cargo run -p ingest --bin reader
```

## Notes
- The v2 L2 message schema may vary; this implementation parses common fields (`bids`, `asks`, `changes`). Adjust mapping if Gemini changes.
- **Messaging Options**: Choose between Kafka (`--features kafka`) or Pulsar (`--features pulsar`). Kafka requires cmake for librdkafka compilation.
- **Shared memory**: Uses volatile reads/writes to avoid locks; ensure only one writer process updates a given file.
- **Features**: 
  - Default build (no messaging): Ingestion only updates shared memory
  - `--features kafka`: Enables Kafka producer/consumer (requires cmake)
  - `--features pulsar`: Enables Pulsar producer/consumer (no cmake required)

## Troubleshooting

**Kafka build issues:**
- Install cmake: `sudo apt-get install cmake` (Ubuntu/Debian)
- Or use Pulsar instead: `--features pulsar`

**Pulsar build issues:**
- Install protobuf compiler: `sudo apt-get install protobuf-compiler` (Ubuntu/Debian)
- Or use Kafka instead: `--features kafka` (but requires cmake)

**Memory-mapped files not found:**
- Run `cargo run -p ingest --bin testdata` to create sample data
- Check `/dev/shm` permissions: `ls -la /dev/shm/`

**WebSocket connection issues:**
- Check internet connectivity and firewall settings
- Gemini API status: https://status.gemini.com/

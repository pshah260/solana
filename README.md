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
- `PG_DSN` (default `host=localhost user=postgres password=postgres dbname=trades`)

### Build

```bash
cargo build --workspace
```

### Run ingestion

```bash
OB_MMAP=/dev/shm/solusd_order_book.mmap \
TOB_MMAP=/dev/shm/solusd_top_of_book.mmap \
KAFKA_BROKERS=localhost:9092 \
KAFKA_TOPIC=gemini.trades \
cargo run -p ingest
```

### Run consumer (Postgres)

```bash
PG_DSN="host=localhost user=postgres password=postgres dbname=trades" \
KAFKA_BROKERS=localhost:9092 \
KAFKA_TOPIC=gemini.trades \
cargo run -p consumer
```

## Notes
- The v2 L2 message schema may vary; this implementation parses common fields (`bids`, `asks`, `changes`). Adjust mapping if Gemini changes.
- For Pulsar, replace Kafka with the `pulsar` crate and a similar producer/consumer wiring.
- Shared memory uses volatile reads/writes to avoid locks; ensure only one writer process updates a given file.

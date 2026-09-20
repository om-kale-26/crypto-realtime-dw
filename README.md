@'
# CryptoStream DW: Real-Time Crypto Data Warehouse

Live Binance trades streamed into ClickHouse, aggregated in real time
with Materialized Views, and visualized in Grafana (5s refresh).

## Architecture
Binance WebSocket -> Python ingestion (batched inserts) -> ClickHouse
(trades_raw -> Materialized View -> candles_1m) -> Grafana / DBeaver

## Tech stack
ClickHouse, Docker Compose, Python, Grafana, DBeaver

## Run it
    docker compose up -d --build

Then open http://localhost:3000 (Grafana) and import grafana/dashboard.json.

## Results
- Symbols tracked: 4 (BTC, ETH, SOL, BNB)
- Throughput: [X] ticks/sec
- End-to-end latency: ~[X] ms from Binance trade to queryable row
- Rows collected: [X]

## Design decisions
- ClickHouse: a columnar database built for fast analytics on event streams.
- Materialized Views instead of scheduled jobs: candles update on every insert, so there is no batch delay.
- Batched inserts: ClickHouse is optimized for batches, so the producer flushes every 100 rows or 1 second.
- Partitioning and TTL: raw ticks are partitioned by day and expire after 14 days.

## Note
Passwords in docker-compose.yml are for local demo use only.

## Future improvements
Kafka between producer and ClickHouse, price-move alerts, a second exchange.
'@ | Set-Content -Path README.md -Encoding ascii
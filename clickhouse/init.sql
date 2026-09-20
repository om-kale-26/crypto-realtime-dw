CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.trades_raw
(
    symbol         LowCardinality(String),
    trade_id       UInt64,
    price          Float64,
    quantity       Float64,
    is_buyer_maker Bool,
    trade_time     DateTime64(3),
    ingested_at    DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (symbol, trade_time)
TTL toDateTime(trade_time) + INTERVAL 14 DAY;

CREATE TABLE IF NOT EXISTS crypto.candles_1m
(
    symbol      LowCardinality(String),
    minute      DateTime,
    open_price  AggregateFunction(argMin, Float64, DateTime64(3)),
    close_price AggregateFunction(argMax, Float64, DateTime64(3)),
    high_price  AggregateFunction(max, Float64),
    low_price   AggregateFunction(min, Float64),
    volume      AggregateFunction(sum, Float64),
    trade_count AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(minute)
ORDER BY (symbol, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.mv_candles_1m
TO crypto.candles_1m
AS
SELECT
    symbol,
    toStartOfMinute(trade_time) AS minute,
    argMinState(price, trade_time) AS open_price,
    argMaxState(price, trade_time) AS close_price,
    maxState(price)  AS high_price,
    minState(price)  AS low_price,
    sumState(quantity) AS volume,
    countState(trade_id) AS trade_count
FROM crypto.trades_raw
GROUP BY symbol, minute;
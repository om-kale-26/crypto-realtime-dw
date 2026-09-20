import asyncio
import os
import time
from datetime import datetime, timezone

import orjson
import websockets
import clickhouse_connect

SYMBOLS = os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

STREAM_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s}@trade" for s in SYMBOLS
)

BATCH_SIZE = 100
FLUSH_INTERVAL = 1.0  # seconds

client = clickhouse_connect.get_client(
    host=CH_HOST,
    port=CH_PORT,
    username="default",
    password=CH_PASSWORD,
    database="crypto",
)

buffer = []


def flush():
    global buffer
    if not buffer:
        return
    client.insert(
        "trades_raw",
        buffer,
        column_names=[
            "symbol", "trade_id", "price", "quantity",
            "is_buyer_maker", "trade_time",
        ],
    )
    print(f"Inserted {len(buffer)} rows")
    buffer = []


async def consume():
    global buffer
    last_flush = time.time()
    async with websockets.connect(STREAM_URL, ping_interval=20) as ws:
        print(f"Connected. Streaming: {SYMBOLS}")
        async for message in ws:
            payload = orjson.loads(message)
            data = payload.get("data", {})
            if not data:
                continue
            row = (
                data["s"].lower(),
                data["t"],
                float(data["p"]),
                float(data["q"]),
                bool(data["m"]),
                datetime.fromtimestamp(data["T"] / 1000.0, tz=timezone.utc),
            )
            buffer.append(row)
            now = time.time()
            if len(buffer) >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL:
                flush()
                last_flush = now


async def main():
    while True:
        try:
            await consume()
        except Exception as e:
            print(f"Connection dropped: {e}. Reconnecting in 3s...")
            try:
                flush()
            except Exception:
                pass
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
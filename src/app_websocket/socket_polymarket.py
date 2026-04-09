import asyncio
import contextlib
import csv
import json
from datetime import UTC, datetime
from pathlib import Path

import websockets


WS_URL = "wss://ws-live-data.polymarket.com"

BASE_DIR = Path("/Users/daniildroncev/Dev/parsing_crypto")
OUTPUT_DIR = BASE_DIR / "data" / "raw" / "polymarket"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DURATION_SECONDS = 60 * 15

SUBSCRIPTION_MESSAGE = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices",
            "type": "update",
        }
    ],
}


async def send_ping(ws):
    while True:
        await asyncio.sleep(5)
        await ws.send("PING")
        print("[PING] sent")


def format_ts(ms: int | None) -> str:
    if not ms:
        return "-"
    dt = datetime.fromtimestamp(ms / 1000, tz=UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f UTC")


def build_output_file() -> Path:
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return OUTPUT_DIR / f"polymarket_btcusdt_{timestamp_str}.csv"


async def main():
    output_file = build_output_file()
    start_monotonic = asyncio.get_running_loop().time()

    with output_file.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["timestamp", "polymarket"])

        async with websockets.connect(
            WS_URL,
            open_timeout=30,
        ) as ws:
            print(f"Connected to {WS_URL}")

            await ws.send(json.dumps(SUBSCRIPTION_MESSAGE))
            print("Subscription sent:")
            print(json.dumps(SUBSCRIPTION_MESSAGE, ensure_ascii=False, indent=2))

            ping_task = asyncio.create_task(send_ping(ws))

            try:
                async for message in ws:
                    if asyncio.get_running_loop().time() - start_monotonic >= DURATION_SECONDS:
                        print("Finished: 60 seconds elapsed")
                        break

                    if not message or not message.strip():
                        continue

                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError:
                        print("Message is not JSON")
                        continue

                    payload = data.get("payload", {})
                    symbol = payload.get("symbol")
                    if symbol != "btcusdt":
                        continue

                    price = payload.get("value")
                    price_timestamp = payload.get("timestamp")

                    if price is None or price_timestamp is None:
                        continue

                    writer.writerow([price_timestamp, price])
                    csv_file.flush()

                    print(
                        f"saved: "
                        f"symbol={symbol} "
                        f"price={price} "
                        f"price_timestamp={format_ts(price_timestamp)}"
                    )

            finally:
                ping_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ping_task

    print(f"CSV saved to: {output_file.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
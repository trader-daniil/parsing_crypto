import asyncio
import csv
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import websockets


WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

BASE_DIR = Path("/Users/daniildroncev/Dev/parsing_crypto")
OUTPUT_DIR = BASE_DIR / "data" / "raw" / "binance"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DURATION_SECONDS = 60 * 15


def build_output_file() -> Path:
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return OUTPUT_DIR / f"binance_btcusdt_{timestamp_str}.csv"


async def main():
    output_file = build_output_file()
    loop = asyncio.get_running_loop()
    started_at = loop.time()

    latest_by_second: dict[int, float] = {}

    async with websockets.connect(WS_URL) as ws:
        print(f"Connected to {WS_URL}")

        async for message in ws:
            if loop.time() - started_at >= DURATION_SECONDS:
                print("Finished: 60 seconds elapsed")
                break

            if not message or not message.strip():
                continue

            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                print("Message is not JSON")
                continue

            # Spot trade stream payload обычно содержит:
            # e - event type
            # E - event time
            # s - symbol
            # p - price
            symbol = data.get("s")
            if symbol != "BTCUSDT":
                continue

            event_time = data.get("E")
            price = data.get("p")

            if event_time is None or price is None:
                continue

            second_ts = (int(event_time) // 1000) * 1000
            latest_by_second[second_ts] = float(Decimal(price))

            print(
                f"saved-in-memory: "
                f"symbol={symbol} "
                f"event_time={event_time} "
                f"second_ts={second_ts} "
                f"price={price}"
            )

    with output_file.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["timestamp", "binance"])

        for ts in sorted(latest_by_second):
            writer.writerow([ts, latest_by_second[ts]])

    print(f"CSV saved to: {output_file.resolve()}")
    print(f"Rows saved: {len(latest_by_second)}")


if __name__ == "__main__":
    asyncio.run(main())
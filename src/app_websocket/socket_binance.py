import asyncio
import csv
import json
import time
from datetime import datetime
from pathlib import Path

import websockets


WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_DIR = BASE_DIR / "data" / "raw" / "binance"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DURATION_SECONDS = 25


def build_output_file() -> Path:
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return OUTPUT_DIR / f"binance_btcusdt_bid_{timestamp_str}.csv"


async def receiver(ws, state: dict):
    async for message in ws:
        if not message or not message.strip():
            continue

        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            continue

        symbol = data.get("s")
        bid = data.get("b")

        if symbol != "BTCUSDT" or bid is None:
            continue

        state["latest_bid"] = bid
        state["last_update_ms"] = time.time_ns() // 1_000_000

        print(
            f"updated-in-memory: "
            f"symbol={symbol} "
            f"bid={bid}"
        )


async def writer(csv_writer, csv_file, state: dict, started_at: float, duration_seconds: int):
    loop = asyncio.get_running_loop()

    while True:
        elapsed = loop.time() - started_at
        if elapsed >= duration_seconds:
            print(f"Finished: {duration_seconds} seconds elapsed")
            break

        now_ms = time.time_ns() // 1_000_000
        second_ts = (now_ms // 1000) * 1000

        latest_bid = state.get("latest_bid")
        if latest_bid is not None:
            csv_writer.writerow([second_ts, latest_bid])
            csv_file.flush()

            print(
                f"saved: "
                f"timestamp={second_ts} "
                f"best_bid={latest_bid}"
            )

        sleep_ms = 1000 - (now_ms % 1000)
        await asyncio.sleep(sleep_ms / 1000)


async def main():
    output_file = build_output_file()
    loop = asyncio.get_running_loop()
    started_at = loop.time()

    state = {
        "latest_bid": None,
        "last_update_ms": None,
    }

    with output_file.open("w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["timestamp", "best_bid"])

        async with websockets.connect(WS_URL) as ws:
            print(f"Connected to {WS_URL}")

            receiver_task = asyncio.create_task(receiver(ws, state))
            writer_task = asyncio.create_task(
                writer(csv_writer, csv_file, state, started_at, DURATION_SECONDS)
            )

            done, pending = await asyncio.wait(
                {receiver_task, writer_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()

            for task in pending:
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    print(f"CSV saved to: {output_file.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
import argparse
import asyncio
import contextlib
import csv
import json
import time
from datetime import datetime
from pathlib import Path

import websockets


WS_URL = "wss://ws-live-data.polymarket.com"

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_DIR = BASE_DIR / "data" / "raw" / "polymarket"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect raw crypto prices from Polymarket RTDS."
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="Symbol in lowercase format, for example: btcusdt",
    )
    parser.add_argument(
        "--duration",
        required=True,
        type=int,
        help="Collection duration in seconds.",
    )
    return parser.parse_args()


def build_subscription_message() -> dict:
    return {
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices",
                "type": "update",
            }
        ],
    }


def build_output_file(symbol: str) -> Path:
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return OUTPUT_DIR / f"polymarket_{symbol}_{timestamp_str}.csv"


async def send_ping(ws):
    while True:
        await asyncio.sleep(5)
        await ws.send("PING")
        print("[PING] sent")


async def receiver(ws, state: dict, expected_symbol: str):
    async for message in ws:
        if not message or not message.strip():
            continue

        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            continue

        payload = data.get("payload", {})
        symbol = payload.get("symbol")

        if symbol != expected_symbol:
            continue

        price = payload.get("value")
        source_timestamp = payload.get("timestamp")

        if price is None:
            continue

        state["latest_price"] = price
        state["source_timestamp"] = source_timestamp
        state["last_update_ms"] = time.time_ns() // 1_000_000

        print(
            f"updated-in-memory: "
            f"symbol={symbol} "
            f"price={price} "
            f"source_timestamp={source_timestamp}"
        )


async def writer(
    csv_writer,
    csv_file,
    state: dict,
    started_at: float,
    duration_seconds: int,
):
    loop = asyncio.get_running_loop()

    while True:
        elapsed = loop.time() - started_at
        if elapsed >= duration_seconds:
            print(f"Finished: {duration_seconds} seconds elapsed")
            break

        now_ms = time.time_ns() // 1_000_000
        second_ts = (now_ms // 1000) * 1000

        latest_price = state.get("latest_price")
        source_timestamp = state.get("source_timestamp")

        if latest_price is not None:
            csv_writer.writerow([second_ts, latest_price, source_timestamp])
            csv_file.flush()

            print(
                f"saved: "
                f"timestamp={second_ts} "
                f"polymarket={latest_price} "
                f"source_timestamp={source_timestamp}"
            )

        sleep_ms = 1000 - (now_ms % 1000)
        await asyncio.sleep(sleep_ms / 1000)


async def main():
    args = parse_args()
    symbol = args.symbol.strip().lower()
    duration_seconds = args.duration

    subscription_message = build_subscription_message()
    output_file = build_output_file(symbol)
    loop = asyncio.get_running_loop()
    started_at = loop.time()

    state = {
        "latest_price": None,
        "source_timestamp": None,
        "last_update_ms": None,
    }

    with output_file.open("w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["timestamp", "polymarket", "source_timestamp"])

        async with websockets.connect(
            WS_URL,
            open_timeout=30,
        ) as ws:
            print(f"Connected to {WS_URL}")

            await ws.send(json.dumps(subscription_message))
            print("Subscription sent:")
            print(json.dumps(subscription_message, ensure_ascii=False, indent=2))

            ping_task = asyncio.create_task(send_ping(ws))
            receiver_task = asyncio.create_task(receiver(ws, state, symbol))
            writer_task = asyncio.create_task(
                writer(
                    csv_writer,
                    csv_file,
                    state,
                    started_at,
                    duration_seconds,
                )
            )

            try:
                done, pending = await asyncio.wait(
                    {receiver_task, writer_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()

                for task in pending:
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
            finally:
                ping_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ping_task

    print(f"CSV saved to: {output_file.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
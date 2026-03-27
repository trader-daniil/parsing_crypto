import asyncio
import json
from datetime import datetime, timezone

from websockets.asyncio.client import connect

WS_URL = "wss://ws-live-data.polymarket.com"


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")


async def ping_loop(ws):
    while True:
        await asyncio.sleep(5)
        await ws.send("PING")
        print(f"[{now()}] -> PING")


async def main():
    print(f"[{now()}] connecting to {WS_URL} ...")

    async with connect(
        WS_URL,
        proxy=None,
        open_timeout=10,
        ping_interval=None,
        ping_timeout=None,
    ) as ws:
        print(f"[{now()}] connected")

        subscribe_msg = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": json.dumps({"symbol": "btc/usd"}),
                }
            ],
        }

        await ws.send(json.dumps(subscribe_msg))
        print(f"[{now()}] subscription sent: {subscribe_msg}")

        ping_task = asyncio.create_task(ping_loop(ws))

        try:
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    print(f"[{now()}] no messages for 15s")
                    continue

                print(f"[{now()}] <- raw: {raw!r}")

                if raw is None:
                    print(f"[{now()}] received None, skip")
                    continue

                if not isinstance(raw, str):
                    print(f"[{now()}] non-text frame: {type(raw)}")
                    continue

                raw = raw.strip()
                if not raw:
                    print(f"[{now()}] empty message, skip")
                    continue

                if raw == "PONG":
                    print(f"[{now()}] <- PONG")
                    continue

                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    print(f"[{now()}] non-json message: {raw!r}")
                    continue

                print(f"[{now()}] parsed: {data}")

                topic = data.get("topic")
                msg_type = data.get("type")
                payload = data.get("payload", {})

                if topic in {"crypto_prices", "crypto_prices_chainlink"}:
                    symbol = payload.get("symbol")
                    price = payload.get("value")
                    ts = payload.get("timestamp")
                    print(
                        f"[{now()}] symbol={symbol} price={price} exchange_ts={ts}"
                    )

        finally:
            ping_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
import asyncio
import json
from websockets.asyncio.client import connect

WS_URL = "wss://ws-live-data.polymarket.com"

async def main():
    async with connect(WS_URL) as ws:
        msg = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices",
                    "type": "update",
                    "filters": "btcusdt"
                }
            ]
        }

        await ws.send(json.dumps(msg))

        while True:
            raw = await ws.recv()
            data = json.loads(raw)

            if data.get("topic") == "crypto_prices" and data.get("type") == "update":
                payload = data.get("payload", {})
                print(
                    f"symbol={payload.get('symbol')} "
                    f"time={payload.get('timestamp')} "
                    f"price={payload.get('value')}"
                )

asyncio.run(main())
import asyncio
import contextlib
import json
import time
from datetime import datetime, timezone

import websockets

last_data = None


async def receiver(symbol: str):
    global last_data
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@bookTicker"

    async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            last_data = data


def ms_to_str(ms: int | None) -> str:
    if ms is None:
        return "None"
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone()
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f %Z")


async def printer(duration_sec: int = 60):
    global last_data

    next_tick = time.time()

    for i in range(duration_sec):
        next_tick += 1
        await asyncio.sleep(max(0, next_tick - time.time()))

        local_dt = datetime.now().astimezone()
        local_str = local_dt.strftime("%Y-%m-%d %H:%M:%S.%f %Z")

        if last_data is None:
            print(f"[{i + 1}] local_time={local_str} data=ещё нет")
            continue

        event_time_ms = last_data.get("E")

        print(
            f"[{i + 1}] "
            f"local_time={local_str} "
            f"event_time={ms_to_str(event_time_ms)} "
            f"event_time_ms={event_time_ms} "
            f"symbol={last_data['s']} "
            f"bid={last_data['b']} ({last_data['B']}) "
            f"ask={last_data['a']} ({last_data['A']})"
        )


async def main():
    symbol = "BTCUSDT"
    recv_task = asyncio.create_task(receiver(symbol))

    try:
        await printer(60)
    finally:
        recv_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await recv_task


if __name__ == "__main__":
    asyncio.run(main())
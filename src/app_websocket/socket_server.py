import asyncio
import websockets


async def handler(websocket):
    async for message in websocket:
        print(f"Получено: {message}")
        await websocket.send(f"Эхо: {message}")

async def main():
    async with websockets.serve(handler, "localhost", 8765):
        print("webSocket сервер запущен на ws://localhost:8765")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())

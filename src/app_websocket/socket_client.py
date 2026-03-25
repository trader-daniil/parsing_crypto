import asyncio
import websockets


async def main():

    url = "ws://localhost:8765"
    async with websockets.connect(url) as websocket:
        await websocket.send("Привет, сервер!")
        response = await websocket.recv()
        print(f"Ответ сервера: {response}")
if __name__ == "__main__":
    asyncio.run(main())

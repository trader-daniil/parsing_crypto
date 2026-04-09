import asyncio
import sys


async def run_module(module_name: str) -> int:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        module_name,
    )

    return_code = await process.wait()
    print(f"[{module_name}] finished with code {return_code}")
    return return_code


async def main():
    polymarket_task = asyncio.create_task(
        run_module("app_websocket.socket_polymarket")
    )
    binance_task = asyncio.create_task(
        run_module("app_websocket.socket_binance")
    )

    polymarket_code, binance_code = await asyncio.gather(
        polymarket_task,
        binance_task,
    )

    if polymarket_code != 0 or binance_code != 0:
        raise SystemExit(
            f"One of the processes failed: "
            f"polymarket={polymarket_code}, binance={binance_code}"
        )

    print("Both processes finished successfully")
    print("Starting plot generation...")

    plot_code = await run_module("app_websocket.plot_from_raw")

    if plot_code != 0:
        raise SystemExit(f"Plot generation failed with code {plot_code}")

    print("Plot generated successfully")


if __name__ == "__main__":
    asyncio.run(main())
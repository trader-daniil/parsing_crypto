import argparse
import asyncio
import sys


SUPPORTED_SYMBOLS = {
    "btcusdt",
    "ethusdt",
    "solusdt",
    "xrpusdt",
}

DEFAULT_SYMBOL = "btcusdt"
DURATION_SECONDS = 25


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Polymarket and Binance collectors, then build plot."
    )
    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
        help=(
            "Trading symbol to process. "
            f"Supported values: {', '.join(sorted(SUPPORTED_SYMBOLS))}. "
            f"Default: {DEFAULT_SYMBOL}"
        ),
    )
    return parser.parse_args()


def validate_symbol(symbol: str) -> str:
    normalized_symbol = symbol.strip().lower()

    if normalized_symbol not in SUPPORTED_SYMBOLS:
        supported = ", ".join(sorted(SUPPORTED_SYMBOLS))
        raise SystemExit(
            f"Unsupported symbol: {symbol!r}. "
            f"Supported symbols: {supported}"
        )

    return normalized_symbol


async def run_module(module_name: str, *module_args: str) -> int:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        module_name,
        *module_args,
    )

    return_code = await process.wait()
    print(f"[{module_name}] finished with code {return_code}")
    return return_code


async def main():
    args = parse_args()
    symbol = validate_symbol(args.symbol)

    print(
        f"Starting pipeline for symbol={symbol}, "
        f"duration={DURATION_SECONDS}s"
    )

    polymarket_task = asyncio.create_task(
        run_module(
            "app_websocket.socket_polymarket",
            "--symbol",
            symbol,
            "--duration",
            str(DURATION_SECONDS),
        )
    )
    binance_task = asyncio.create_task(
        run_module(
            "app_websocket.socket_binance",
            "--symbol",
            symbol,
            "--duration",
            str(DURATION_SECONDS),
        )
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

    plot_code = await run_module(
        "app_websocket.plot_from_raw",
        "--symbol",
        symbol,
    )

    if plot_code != 0:
        raise SystemExit(f"Plot generation failed with code {plot_code}")

    print("Plot generated successfully")


if __name__ == "__main__":
    asyncio.run(main())
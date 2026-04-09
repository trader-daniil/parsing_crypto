import csv
from datetime import UTC, datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter


BASE_DIR = Path("/Users/daniildroncev/Dev/parsing_crypto")
BINANCE_DIR = BASE_DIR / "data" / "raw" / "binance"
POLYMARKET_DIR = BASE_DIR / "data" / "raw" / "polymarket"
PLOTS_DIR = BASE_DIR / "data" / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def get_latest_file(directory: Path, pattern: str) -> Path:
    files = sorted(directory.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found in {directory} by pattern {pattern}")
    return files[-1]


def read_single_column_csv(file_path: Path, value_column: str) -> dict[int, float]:
    result: dict[int, float] = {}

    with file_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            timestamp_raw = row.get("timestamp")
            value_raw = row.get(value_column)

            if not timestamp_raw or not value_raw:
                continue

            result[int(timestamp_raw)] = float(value_raw)

    return result


def build_full_second_range(
    polymarket_data: dict[int, float],
    binance_data: dict[int, float],
) -> list[int]:
    all_keys = sorted(set(polymarket_data) | set(binance_data))
    if not all_keys:
        return []

    start_ts = all_keys[0]
    end_ts = all_keys[-1]

    return list(range(start_ts, end_ts + 1000, 1000))


def forward_fill_by_timestamps(
    timestamps: list[int],
    values: dict[int, float],
) -> list[float | None]:
    result: list[float | None] = []
    last_value: float | None = None

    for ts in timestamps:
        if ts in values:
            last_value = values[ts]
        result.append(last_value)

    return result


def build_plot_output_path(polymarket_file: Path, binance_file: Path) -> Path:
    return PLOTS_DIR / f"plot_{polymarket_file.stem}__{binance_file.stem}.png"


def main():
    polymarket_file = get_latest_file(POLYMARKET_DIR, "polymarket_btcusdt_*.csv")
    binance_file = get_latest_file(BINANCE_DIR, "binance_btcusdt_*.csv")

    print(f"Using Polymarket file: {polymarket_file}")
    print(f"Using Binance file: {binance_file}")

    polymarket_data = read_single_column_csv(polymarket_file, "polymarket")
    binance_data = read_single_column_csv(binance_file, "binance")

    timestamps_ms = build_full_second_range(polymarket_data, binance_data)
    if not timestamps_ms:
        raise ValueError("No data to plot")

    timestamps_dt = [
        datetime.fromtimestamp(ts / 1000, tz=UTC)
        for ts in timestamps_ms
    ]

    polymarket_values = forward_fill_by_timestamps(timestamps_ms, polymarket_data)
    binance_values = forward_fill_by_timestamps(timestamps_ms, binance_data)

    output_file = build_plot_output_path(polymarket_file, binance_file)

    fig, ax = plt.subplots(figsize=(14, 7))

    ax.plot(timestamps_dt, polymarket_values, label="Polymarket")
    ax.plot(timestamps_dt, binance_values, label="Binance")

    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Price")
    ax.set_title("BTCUSDT: Polymarket vs Binance")
    ax.legend()
    ax.grid(True)

    # Показываем на оси X часы:минуты:секунды
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S", tz=UTC))
    fig.autofmt_xdate(rotation=45)

    # Показываем на оси Y полные значения с 2 знаками после точки
    ax.yaxis.set_major_formatter(FormatStrFormatter("%.2f"))

    plt.tight_layout()
    plt.savefig(output_file, dpi=150)
    plt.close()

    print(f"Plot saved to: {output_file.resolve()}")


if __name__ == "__main__":
    main()
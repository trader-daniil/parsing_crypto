import csv
from datetime import UTC, datetime
from pathlib import Path

import matplotlib.pyplot as plt


BASE_DIR = Path("/Users/daniildroncev/Dev/parsing_crypto")
COMBINED_DIR = BASE_DIR / "data" / "raw" / "combined"
PLOTS_DIR = BASE_DIR / "data" / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def get_latest_file(directory: Path, pattern: str) -> Path:
    files = sorted(directory.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found in {directory} by pattern {pattern}")
    return files[-1]


def read_combined_csv(file_path: Path):
    timestamps = []
    polymarket_values = []
    binance_values = []

    with file_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            timestamp_raw = row.get("timestamp")
            polymarket_raw = row.get("polymarket")
            binance_raw = row.get("binance")

            if not timestamp_raw:
                continue

            timestamp_ms = int(timestamp_raw)
            timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)

            timestamps.append(timestamp_dt)
            polymarket_values.append(float(polymarket_raw) if polymarket_raw else None)
            binance_values.append(float(binance_raw) if binance_raw else None)

    return timestamps, polymarket_values, binance_values


def build_plot_output_path(source_file: Path) -> Path:
    return PLOTS_DIR / f"{source_file.stem}.png"


def main():
    input_file = get_latest_file(COMBINED_DIR, "btcusdt_polymarket_binance_*.csv")
    output_file = build_plot_output_path(input_file)

    timestamps, polymarket_values, binance_values = read_combined_csv(input_file)

    plt.figure(figsize=(14, 7))
    plt.plot(timestamps, polymarket_values, label="Polymarket")
    plt.plot(timestamps, binance_values, label="Binance")

    plt.xlabel("Time (UTC)")
    plt.ylabel("Price")
    plt.title("BTCUSDT: Polymarket vs Binance")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig(output_file, dpi=150)
    plt.close()

    print(f"Input CSV: {input_file}")
    print(f"Plot saved to: {output_file.resolve()}")


if __name__ == "__main__":
    main()
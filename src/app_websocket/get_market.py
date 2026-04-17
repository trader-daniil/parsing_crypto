from datetime import datetime, UTC
from zoneinfo import ZoneInfo
import json

import requests


def get_btc_5m_slug(dt: datetime | None = None) -> str:
    """
    Генерирует slug вида btc-updown-5m-<timestamp>
    для текущего или переданного времени.
    """
    if dt is None:
        dt = datetime.now(UTC)

    ny_tz = ZoneInfo("America/New_York")
    dt_ny = dt.astimezone(ny_tz)

    minute = (dt_ny.minute // 5) * 5
    dt_floor = dt_ny.replace(minute=minute, second=0, microsecond=0)

    dt_utc = dt_floor.astimezone(UTC)
    ts = int(dt_utc.timestamp())

    return f"btc-updown-5m-{ts}"


def fetch_event_by_slug(slug: str) -> dict | None:
    """
    Делает запрос к Polymarket по slug и возвращает JSON-ответ.
    Если событие не найдено, возвращает None.
    """
    url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
    response = requests.get(url, timeout=10)

    if response.status_code == 404:
        return None

    response.raise_for_status()
    return response.json()


def extract_clob_token_ids(event_data: dict) -> list[str]:
    """
    Достаёт clobTokenIds из первого market внутри event_data
    и возвращает их как список строк.
    """
    markets = event_data.get("markets", [])
    if not markets:
        return []

    clob_token_ids_raw = markets[0].get("clobTokenIds")
    if not clob_token_ids_raw:
        return []

    return json.loads(clob_token_ids_raw)


def main() -> None:
    slug = get_btc_5m_slug()
    data = fetch_event_by_slug(slug)

    if data is None:
        print("NOT FOUND:", slug)
        return

    print("FOUND:", slug)

    token_ids = extract_clob_token_ids(data)

    if not token_ids:
        print("clobTokenIds not found")
        return

    print("clobTokenIds:")
    for token_id in token_ids:
        print(token_id)


if __name__ == "__main__":
    main()
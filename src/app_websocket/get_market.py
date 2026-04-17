from datetime import datetime, timedelta, UTC
from zoneinfo import ZoneInfo
import requests



def get_btc_5m_slug(dt: datetime | None = None) -> str:
    """
    Генерирует slug вида btc-updown-5m-<timestamp>
    для текущего (или переданного) времени
    """

    # 1. текущее время (UTC)
    if dt is None:
        dt = datetime.now(UTC)

    # 2. переводим в New York (ET с учетом DST)
    ny_tz = ZoneInfo("America/New_York")
    dt_ny = dt.astimezone(ny_tz)

    # 3. округляем ВНИЗ до 5 минут
    minute = (dt_ny.minute // 5) * 5
    dt_floor = dt_ny.replace(minute=minute, second=0, microsecond=0)

    # 4. обратно в UTC
    dt_utc = dt_floor.astimezone(UTC)

    # 5. timestamp
    ts = int(dt_utc.timestamp())

    # 6. slug
    return f"btc-updown-5m-{ts}"

slug = get_btc_5m_slug()

url = f"https://gamma-api.polymarket.com/events/slug/{slug}"

response = requests.get(url, timeout=10)

if response.status_code == 200:
    data = response.json()
    print("FOUND:", slug)

    for market in data.get("markets", []):
        print(market["id"], "|", market["question"])
else:
    print("NOT FOUND:", slug)
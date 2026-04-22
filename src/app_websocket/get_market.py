from dataclasses import dataclass
from datetime import datetime, UTC
from zoneinfo import ZoneInfo
import json

import requests


@dataclass
class OutcomeTokens:
    token_up: str
    token_down: str


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


def extract_outcome_tokens(event_data: dict) -> OutcomeTokens | None:
    """
    Достаёт токены Up / Down из первого market внутри event_data
    и возвращает их в виде dataclass.
    """
    markets = event_data.get("markets", [])
    if not markets:
        return None

    market = markets[0]

    outcomes_raw = market.get("outcomes")
    clob_token_ids_raw = market.get("clobTokenIds")

    if not outcomes_raw or not clob_token_ids_raw:
        return None

    outcomes = json.loads(outcomes_raw)
    token_ids = json.loads(clob_token_ids_raw)

    if len(outcomes) != 2 or len(token_ids) != 2:
        return None

    outcome_to_token = dict(zip(outcomes, token_ids, strict=False))

    token_up = outcome_to_token.get("Up")
    token_down = outcome_to_token.get("Down")

    if not token_up or not token_down:
        return None

    return OutcomeTokens(
        token_up=token_up,
        token_down=token_down,
    )

def fetch_order_book(token_id: str) -> dict | None:
    """
    Получим bid и ask для конкретного события по 
    переданному токену.
    """
    url = "https://clob.polymarket.com/book"

    params = {
        "token_id": token_id
    }

    response = requests.get(url, params=params, timeout=10)

    if response.status_code == 404:
        return None

    response.raise_for_status()
    return response.json()

def extract_best_bid_ask(book: dict) -> tuple[float | None, float | None]:
    """
    Из стакана достаем лучшие bid и ask.
    """
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    print(bids, '     bids')
    print(asks, '  asks')

    best_bid = float(bids[0]["price"]) if bids else None
    best_ask = float(asks[0]["price"]) if asks else None

    return best_bid, best_ask


def main() -> None:
    slug = get_btc_5m_slug()
    data = fetch_event_by_slug(slug)

    if data is None:
        print("NOT FOUND:", slug)
        return

    print("FOUND:", slug)

    outcome_tokens = extract_outcome_tokens(data)
    if outcome_tokens is None:
        print("Outcome tokens not found")
        return


    book_up = fetch_order_book(outcome_tokens.token_up)
    book_down = fetch_order_book(outcome_tokens.token_down)

    if book_up:
        bid, ask = extract_best_bid_ask(book_up)
        print("UP  | bid:", bid, "ask:", ask)

    if book_down:
        bid, ask = extract_best_bid_ask(book_down)
        print("DOWN| bid:", bid, "ask:", ask)


if __name__ == "__main__":
    main()
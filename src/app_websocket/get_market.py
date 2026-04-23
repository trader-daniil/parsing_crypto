import asyncio
from dataclasses import dataclass
from datetime import datetime, UTC
from zoneinfo import ZoneInfo
import json

import requests
import websockets

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


async def stream_market_channel(outcome_tokens: OutcomeTokens) -> None:
    """Подклбчению к потоку оредов по нашим токенам- Up и Down."""
    ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    subscribe_message = {
        "assets_ids": [
            outcome_tokens.token_up,
            outcome_tokens.token_down,
        ],
        "type": "market",
        "initial_dump": True,
        "level": 2,
        "custom_feature_enabled": True,
    }

    async with websockets.connect(ws_url) as websocket:
        await websocket.send(json.dumps(subscribe_message))
        print("Subscribed to market channel")

        async def ping_loop() -> None:
            while True:
                await asyncio.sleep(10)
                await websocket.send("PING")

        ping_task = asyncio.create_task(ping_loop())

        try:
            while True:
                raw_message = await websocket.recv()

                if raw_message == "PONG":
                    continue

                payload = json.loads(raw_message)
                messages = payload if isinstance(payload, list) else [payload]

                for message in messages:
                    if not isinstance(message, dict):
                        continue

                    handle_market_message(message, outcome_tokens)
        finally:
            ping_task.cancel()

def extract_best_bid_ask(book: dict) -> tuple[float | None, float | None]:
    """
    Из стакана достаем лучшие bid и ask.
    Не полагаемся на порядок элементов в массиве.
    """
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    best_bid = max((float(level["price"]) for level in bids), default=None)
    best_ask = min((float(level["price"]) for level in asks), default=None)

    return best_bid, best_ask

def handle_market_message(
    message: dict,
    outcome_tokens: OutcomeTokens,
) -> None:
    """
    По вебсокеты получили сообщение о рынке
    Нужно его обработать, вытащить лучшие bid и ask.
    """
    asset_id = message.get("asset_id")
    if asset_id == outcome_tokens.token_up:
        side_name = "UP"
    elif asset_id == outcome_tokens.token_down:
        side_name = "DOWN"
    else:
        return

    event_type = message.get("event_type")

    if event_type == "book":
        best_bid, best_ask = extract_best_bid_ask_from_book(message)
        print(f"{side_name}  | bid: {best_bid} ask: {best_ask}")

    elif event_type == "best_bid_ask":
        best_bid_raw = message.get("best_bid")
        best_ask_raw = message.get("best_ask")

        best_bid = float(best_bid_raw) if best_bid_raw is not None else None
        best_ask = float(best_ask_raw) if best_ask_raw is not None else None

        print(f"{side_name}  | bid: {best_bid} ask: {best_ask}")

def extract_best_bid_ask_from_book(message: dict) -> tuple[float | None, float | None]:
    """
    Внутри JSON, который мы получаем с market channel
    нужно вытащить самые лучшие значения bid и ask.
    """
    bids = message.get("bids", [])
    asks = message.get("asks", [])

    best_bid = max((float(level["price"]) for level in bids), default=None)
    best_ask = min((float(level["price"]) for level in asks), default=None)

    return best_bid, best_ask



async def main() -> None:
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

    print("UP token:", outcome_tokens.token_up)
    print("DOWN token:", outcome_tokens.token_down)
    await stream_market_channel(outcome_tokens)




if __name__ == "__main__":
    asyncio.run(main())
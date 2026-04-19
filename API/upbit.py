"""
API/upbit.py

Рабочий эндпоинт: https://api-manager.upbit.com/api/v1/announcements
  ↑ это /announcements, НЕ /notices — именно здесь была корневая ошибка.

Логика запроса 1:1 из fetch_upbit_announcements (DEFAULT_CONFIG):
  params:  category="trade", os="web"
  key:     data["data"]["notices"]
  ts:      notice["listed_at"]
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from _math import ListingEvent
from c_log import get_logger
from config import UpbitConfig

logger = get_logger("api.upbit")


def _parse_iso_to_ms(dt_str: str) -> int:
    if not dt_str:
        return 0
    try:
        dt = datetime.fromisoformat(dt_str)
        return int(dt.astimezone(timezone.utc).timestamp() * 1000)
    except Exception:
        try:
            clean = dt_str[:19]
            dt = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S")
            return int((dt.timestamp() - 9 * 3600) * 1000)
        except Exception:
            return 0


def _format_time_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def _extract_symbol(title: str) -> Optional[str]:
    match = re.search(r"\(([^)]+)\)", title)
    if match:
        symbol = match.group(1).strip().upper()
        symbol = re.sub(r"\(.*\)", "", symbol).strip()
        if symbol:
            return symbol
    if "for" in title.lower():
        idx = title.lower().find("for")
        tail = title[idx + 3:].strip()
        if tail:
            return tail.split()[0].upper()
    return None


def _to_phemex(sym: str) -> str:
    return f"{sym.upper()}USDT"


class UpbitParser:
    def __init__(self, cfg: UpbitConfig) -> None:
        self._cfg = cfg

    async def fetch_listings(
        self, session: aiohttp.ClientSession
    ) -> list[ListingEvent]:
        listings: list[dict] = []
        page = 1

        while page <= self._cfg.max_pages:
            # Строго как в u_bt2
            params = {
                "category": "trade",
                "page": page,
                "per_page": self._cfg.per_page,  # Здесь теперь будет строго 20
                "os": "web"
            }
            try:
                async with session.get(self._cfg.notice_url, params=params) as resp:
                    if resp.status != 200:
                        logger.error(f"Upbit API error {resp.status} on page {page}")
                        break
                    data = await resp.json()

                notices = data.get("data", {}).get("notices", [])
                if not notices:
                    logger.info(f"No more notices on page {page}")
                    break

                for notice in notices:
                    title = notice.get("title", "")
                    
                    # Проверка по ключевым словам как в u_bt2
                    if not any(kw in title for kw in self._cfg.listing_keywords):
                        continue

                    symbol = _extract_symbol(title)
                    if not symbol:
                        logger.warning(f"Could not extract symbol from: {title!r}")
                        continue

                    listed_at = notice.get("listed_at")
                    if not listed_at:
                        logger.warning(f"No listed_at for {symbol!r}")
                        continue

                    try:
                        announce_ms = _parse_iso_to_ms(listed_at)
                    except Exception as e:
                        logger.warning(f"Failed to parse {listed_at!r}: {e}")
                        continue

                    listings.append({
                        "symbol":          symbol,
                        "phemex_symbol":   _to_phemex(symbol),
                        "announce_ts_ms":  announce_ms,
                        "announce_ts_str": _format_time_utc(announce_ms),
                        "title":           title,
                    })

                logger.info(
                    f"Page {page}: processed {len(notices)} notices, "
                    f"found {len(listings)} listings so far"
                )
                page += 1
                await asyncio.sleep(self._cfg.request_delay_sec)

            except Exception as ex:
                logger.exception(f"Error fetching page {page}: {ex}")
                break

        logger.info(f"Total listing announcements collected: {len(listings)}")

        # Дальше идет твоя логика дедупликации
        seen: set[tuple[str, int]] = set()
        events: list[ListingEvent] = []
        for item in listings:
            key = (item["phemex_symbol"], item["announce_ts_ms"])
            if key not in seen:
                seen.add(key)
                events.append(ListingEvent(
                    symbol=item["symbol"],
                    phemex_symbol=item["phemex_symbol"],
                    announce_ts_ms=item["announce_ts_ms"],
                    announce_ts_str=item["announce_ts_str"],
                    title=item["title"],
                ))

        events.sort(key=lambda e: e.announce_ts_ms)
        logger.info(f"Upbit parser: {len(events)} уникальных листинг-событий")
        return events

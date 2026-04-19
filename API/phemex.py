"""
api/phemex.py — Async адаптер Phemex public API.

Методы:
  update_instruments()    — загрузка активных USDT perpetuals
  get_klines_last()       — последние N свечей
  get_klines_by_time()    — свечи в окне [from_ts, to_ts] (для бэктеста)
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp
import pandas as pd

from c_log import get_logger
from config import PhemexConfig
from const import PHEMEX_ALLOWED_LIMITS, RESOLUTION_MAP

logger = get_logger("api.phemex")

_TIMEOUT = aiohttp.ClientTimeout(total=20)


class PhemexAdapter:
    """Stateful адаптер: хранит инструменты, управляет rate-limit'ом."""

    _PRODUCTS_URL   = "/public/products"
    _KLINE_LAST_URL = "/exchange/public/md/v2/kline/last"
    _KLINE_HIST_URL = "/exchange/public/md/v2/kline"

    def __init__(self, cfg: PhemexConfig) -> None:
        self._cfg = cfg
        self._base = cfg.base_url
        self._lock = asyncio.Lock()
        self._last_kline_ts: float = 0.0

        self.instruments: dict[str, dict] = {}
        self.symbols: set[str] = set()

    # ── Instruments ──────────────────────────────────────────────────────────

    async def update_instruments(self, session: aiohttp.ClientSession) -> None:
        """Загружает список активных USDT perpetual символов."""
        url = f"{self._base}{self._PRODUCTS_URL}"
        try:
            async with session.get(url, timeout=_TIMEOUT) as resp:
                if resp.status != 200:
                    logger.error(f"Phemex products HTTP {resp.status}")
                    return
                data = await resp.json()
        except Exception as ex:
            logger.exception(f"update_instruments: {ex}")
            return

        root = data.get("data", {})
        arr = root.get("perpProductsV2") or root.get("perpProducts") or []

        instruments: dict[str, dict] = {}
        for item in arr:
            if not isinstance(item, dict):
                continue
            sym = str(item.get("symbol", "")).strip()
            quote = str(
                item.get("quoteCurrency") or item.get("settleCurrency") or ""
            ).upper()
            status = str(item.get("status") or item.get("state") or "").lower()
            dead = ("delist", "suspend", "pause", "settle", "close", "expired")
            if sym and not sym.startswith("s") and quote == "USDT":
                if not any(w in status for w in dead):
                    sym_u = sym.upper()
                    instruments[sym_u] = {
                        **item,
                        "symbol": sym_u,
                        "_scale": float(item.get("priceScale", 10_000.0)),
                    }

        if not instruments:
            logger.warning("Phemex: нет активных USDT perpetuals")
            return

        self.instruments = instruments
        self.symbols = set(instruments.keys())
        logger.info(f"Phemex: загружено {len(self.symbols)} USDT perpetuals")

    # ── Kline fetchers ───────────────────────────────────────────────────────

    async def get_klines_last(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        interval: str,
        limit: int,
    ) -> pd.DataFrame:
        """Последние limit свечей (публичный эндпоинт /kline/last)."""
        await self._throttle()
        resolution = RESOLUTION_MAP.get(interval, 60)
        valid_limit = next((l for l in PHEMEX_ALLOWED_LIMITS if l >= limit), 1000)
        params = {"symbol": symbol, "resolution": resolution, "limit": valid_limit}
        url = f"{self._base}{self._KLINE_LAST_URL}"
        return await self._fetch_klines(session, url, params, symbol)

    async def get_klines_by_time(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        interval: str,
        from_ts_sec: int,
        to_ts_sec: int,
    ) -> pd.DataFrame:
        """Свечи в окне [from_ts_sec, to_ts_sec] — для исторического бэктеста."""
        await self._throttle()
        resolution = RESOLUTION_MAP.get(interval, 60)
        params = {
            "symbol": symbol,
            "resolution": resolution,
            "from": from_ts_sec,
            "to": to_ts_sec,
        }
        url = f"{self._base}{self._KLINE_HIST_URL}"
        return await self._fetch_klines(session, url, params, symbol)

    # ── Internal ─────────────────────────────────────────────────────────────

    async def _fetch_klines(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict,
        symbol: str,
    ) -> pd.DataFrame:
        try:
            async with session.get(url, params=params, timeout=_TIMEOUT) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"klines {symbol} HTTP {resp.status}: {text[:200]}")
                    return self._empty()
                data = await resp.json()
        except Exception as ex:
            logger.exception(f"_fetch_klines {symbol}: {ex}")
            return self._empty()

        return self._parse(data, symbol)

    def _parse(self, data: dict, symbol: str) -> pd.DataFrame:
        """
        Phemex kline row layout (v2):
        [ts_sec, open_Ep, high_Ep, low_Ep, volume, turnover, close_Ep]
        index 6 = close (Ep — scaled integer).
        """
        rows = data.get("data", {}).get("rows", [])
        if not rows:
            return self._empty()

        inst = self.instruments.get(symbol, {})
        scale = inst.get("_scale", 0.0)
        if scale <= 0:
            tick = float(inst.get("tickSize", 0.0001))
            scale = 1.0 / tick if tick > 0 else 10_000.0

        parsed: list[tuple[int, float]] = []
        for r in rows:
            if len(r) >= 7:
                try:
                    parsed.append((int(r[0]), float(r[6]) / scale))
                except (TypeError, ValueError):
                    pass

        if not parsed:
            return self._empty()

        df = pd.DataFrame(parsed, columns=["Time", "Close"])
        df["Time"] = pd.to_datetime(df["Time"], unit="s", utc=True)
        df.set_index("Time", inplace=True)
        df.sort_index(inplace=True)
        return df

    async def _throttle(self) -> None:
        async with self._lock:
            elapsed = time.monotonic() - self._last_kline_ts
            gap = self._cfg.kline_interval_sec - elapsed
            if gap > 0:
                await asyncio.sleep(gap)
            self._last_kline_ts = time.monotonic()

    @staticmethod
    def _empty() -> pd.DataFrame:
        return pd.DataFrame(columns=["Close"])

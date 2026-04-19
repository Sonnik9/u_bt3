"""
api/phemex.py — Рабочий адаптер Phemex public API (ported from u_bt2).
"""
from __future__ import annotations

import asyncio
import time
import inspect
from typing import Optional, TYPE_CHECKING

import aiohttp
import pandas as pd

from c_log import get_logger
from config import PhemexConfig


logger = get_logger("api.phemex")


class PhemexAdapter:
    def __init__(self, cfg: PhemexConfig) -> None:
        self._cfg = cfg
        self.base_url = cfg.base_url
        self.exchangeInfo_url = f'{self.base_url}/public/products'
        self.klines_url_last = f'{self.base_url}/exchange/public/md/v2/kline/last'
        self.klines_url_hist = f'{self.base_url}/exchange/public/md/v2/kline'

        self.filtered_symbols: set[str] = set()
        self.symbols: set[str] = set() # Alias для совместимости с u_bt3
        self.instruments: dict[str, dict] = {}

        # Лимитер запросов (Rate Limiter) для защиты от бана API
        self._kline_lock = asyncio.Lock()
        self._last_kline_time = 0.0
        self.kline_interval = cfg.kline_interval_sec

    # ── 1. Instruments ───────────────────────────────────────────────────────

    async def update_instruments(self, session: aiohttp.ClientSession) -> None:
        """Получаем список доступных торговых символов PERPETUAL USDT"""
        try:
            async with session.get(self.exchangeInfo_url) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch exchange info: {response.status}")
                    return
                data = await response.json()
                
            root = data.get("data", {})
            arr = root.get("perpProductsV2") or root.get("perpProducts") or []
            
            instruments = {}
            for item in arr:
                if not isinstance(item, dict): continue
                
                sym = str(item.get("symbol", "")).strip()
                quote = str(item.get("quoteCurrency") or item.get("settleCurrency") or "").upper().strip()
                
                # Твоя правильная проверка статусов
                status = str(item.get("status") or item.get("state") or item.get("symbolStatus") or "").strip().lower()
                is_active = not any(word in status for word in ("delist", "suspend", "pause", "settle", "close", "expired"))
                
                if sym and not sym.startswith("s") and quote == "USDT" and is_active:
                    sym_u = sym.upper()
                    instruments[sym_u] = {
                        **item,
                        "symbol": sym_u,
                        "_parsed_price_scale": float(item.get("priceScale", 10000.0))
                    }
                    
            if not instruments: 
                logger.warning("No perpetual USDT symbols found in exchange info")
                return  
                
            self.instruments = instruments
            self.filtered_symbols = set(self.instruments.keys())
            self.symbols = self.filtered_symbols
            logger.info(f"Phemex: загружено {len(self.symbols)} USDT perpetuals")
            
        except Exception as ex:
            logger.exception(f"{ex} in {inspect.currentframe().f_code.co_name}")

    # ── 2. Kline fetchers ────────────────────────────────────────────────────

    async def get_klines_by_time(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        interval: str,
        from_ts_sec: int,
        to_ts_sec: int,
    ) -> pd.DataFrame:
        """Свечи в окне [from_ts_sec, to_ts_sec] — для бэктеста старых листингов."""
        await self._throttle()
        
        res_map = {
            "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
            "1h": 3600, "4h": 14400, "1d": 86400
        }
        resolution = res_map.get(interval, 60)
        
        params = {
            "symbol": symbol, 
            "resolution": int(resolution), 
            "from": from_ts_sec,
            "to": to_ts_sec
        }
        return await self._fetch_and_parse(session, self.klines_url_hist, params, symbol)

    async def get_klines_last(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        interval: str,
        limit: int,
    ) -> pd.DataFrame:
        """Последние limit свечей (твой get_klines_basic из u_bt2)."""
        await self._throttle()
        
        res_map = {
            "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
            "1h": 3600, "4h": 14400, "1d": 86400
        }
        resolution = res_map.get(interval, 60)
        
        # Узкое горлышко исправлено: Phemex принимает только строгие значения limit
        allowed_limits = [5, 10, 50, 100, 500, 1000]
        valid_limit = next((l for l in allowed_limits if l >= limit), 1000)
        
        params = {
            "symbol": symbol, 
            "resolution": int(resolution), 
            "limit": valid_limit
        }
        return await self._fetch_and_parse(session, self.klines_url_last, params, symbol)

    # ── 3. Internal ──────────────────────────────────────────────────────────

    async def _fetch_and_parse(
        self, session: aiohttp.ClientSession, url: str, params: dict, symbol: str
    ) -> pd.DataFrame:
        """Единый выстраданный парсер свечей"""
        try:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.error(f"Failed klines HTTP {response.status}: {text[:100]} [{symbol}]")
                    return pd.DataFrame(columns=['Close'])

                data = await response.json()
                
            rows = data.get("data", {}).get("rows", [])
            if not rows: 
                return pd.DataFrame(columns=['Close'])

            # ОПРЕДЕЛЯЕМ МАСШТАБ (SCALE) БЕЗОПАСНО
            inst = self.instruments.get(symbol, {})
            scale = inst.get("_parsed_price_scale", 0)
            
            if scale <= 0:
                tick = float(inst.get("tickSize", 0.0001))
                scale = 1.0 / tick if tick > 0 else 10000.0

            parsed_data = []
            for r in rows:
                if len(r) >= 7:
                    # r[0] - timestamp, r[6] - close (в формате Ep)
                    parsed_data.append([int(r[0]), float(r[6]) / scale])

            df = pd.DataFrame(parsed_data, columns=['Time', 'Close'])
            df['Time'] = pd.to_datetime(df['Time'], unit='s', utc=True)
            df.set_index('Time', inplace=True)
            
            # ФИКС РЕВЕРСА: Сортируем время от прошлого к настоящему!
            df.sort_index(inplace=True)
            return df

        except Exception as ex:
            logger.exception(f"{ex} in fetch_and_parse")
            return pd.DataFrame(columns=['Close'])

    async def _throttle(self) -> None:
        """Защита от спама (Rate Limiting)"""
        async with self._kline_lock:
            elapsed = time.monotonic() - self._last_kline_time
            if elapsed < self.kline_interval:
                await asyncio.sleep(self.kline_interval - elapsed)
            self._last_kline_time = time.monotonic()
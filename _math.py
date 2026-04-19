"""
_math.py — Весь математический слой:
  • ListingEvent / DeltaResult / MetricsResult  (контекстные датаклассы)
  • compute_rsi()      — Wilder RSI
  • compute_delta()    — Δ% по двум точкам DataFrame
  • TrendSignal        — EMA-кросс тренд
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from c_log import get_logger
from config import BacktestConfig

logger = get_logger("_math")


# ══════════════════════════════════════════════════════════════════
#  Контекстные датаклассы (DTO)
# ══════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class ListingEvent:
    """Одно объявление листинга с Upbit."""
    symbol: str           # тикер на Upbit,  например "ORDI"
    phemex_symbol: str    # тикер на Phemex,  например "ORDIUSDT"
    announce_ts_ms: int   # UTC ms
    announce_ts_str: str  # "2024-01-01 12:00:00 UTC"
    title: str            # оригинальный корейский заголовок


@dataclass(frozen=True)
class DeltaResult:
    """Результат замера Δ цены."""
    symbol: str
    phemex_symbol: str
    announce_ts_ms: int
    announce_ts_str: str
    price_t0: float
    price_tn: float
    delta_pct: float
    delta_minutes: int


@dataclass
class MetricsResult:
    """Финальный результат: Δ + RSI + тренд."""
    rank: int
    symbol: str
    phemex_symbol: str
    announce_ts_ms: int
    announce_ts_str: str
    price_t0: float
    price_tn: float
    delta_pct: float
    delta_minutes: int
    rsi: Optional[float]
    trend: Optional[str]
    klines_count: int

    def to_dict(self) -> dict:
        return {
            "rank":             self.rank,
            "symbol":           self.symbol,
            "phemex_symbol":    self.phemex_symbol,
            "announce_ts_ms":   self.announce_ts_ms,
            "announce_ts_str":  self.announce_ts_str,
            "price_t0":         self.price_t0,
            "price_tn":         self.price_tn,
            "delta_pct":        self.delta_pct,
            "delta_minutes":    self.delta_minutes,
            "rsi":              self.rsi,
            "trend":            self.trend,
            "klines_count":     self.klines_count,
        }


# ══════════════════════════════════════════════════════════════════
#  RSI (Wilder)
# ══════════════════════════════════════════════════════════════════

def compute_rsi(series: pd.Series, window: int = 14) -> Optional[float]:
    """
    Wilder RSI. Возвращает последнее значение или None при нехватке данных.
    EWM с com = window-1 → соответствует сглаживанию Уайлдера.
    """
    if len(series) < window + 1:
        logger.debug(f"RSI({window}): need {window+1} bars, got {len(series)}")
        return None

    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    avg_gain = gain.ewm(com=window - 1, min_periods=window).mean()
    avg_loss = loss.ewm(com=window - 1, min_periods=window).mean()

    rs = avg_gain / avg_loss.replace(0.0, float("inf"))
    rsi_series = 100.0 - (100.0 / (1.0 + rs))

    last = rsi_series.iloc[-1]
    return float(last) if not pd.isna(last) else None


# ══════════════════════════════════════════════════════════════════
#  Delta
# ══════════════════════════════════════════════════════════════════

def _nearest_close(
    df: pd.DataFrame,
    target_dt: datetime,
    tolerance_sec: int = 300,
) -> Optional[float]:
    """
    Возвращает Close первой свечи с timestamp >= target_dt.
    Fallback: последняя свеча до target_dt если она в пределах tolerance_sec.
    """
    forward = df[df.index >= target_dt]
    if not forward.empty:
        return float(forward.iloc[0]["Close"])

    backward = df[df.index < target_dt]
    if not backward.empty:
        gap = (target_dt - backward.index[-1]).total_seconds()
        if gap <= tolerance_sec:
            return float(backward.iloc[-1]["Close"])
    return None


def compute_delta(
    event: ListingEvent,
    df: pd.DataFrame,
    delta_minutes: int,
) -> Optional[DeltaResult]:
    """
    Считает Δ% между T0 (анонс) и T0+N минут.
    df должен иметь UTC DatetimeIndex и колонку 'Close'.
    """
    if df.empty or "Close" not in df.columns:
        return None

    if df.index.tzinfo is None:
        df = df.copy()
        df.index = df.index.tz_localize("UTC")

    t0 = datetime.fromtimestamp(event.announce_ts_ms / 1000, tz=timezone.utc)
    tn = t0 + timedelta(minutes=delta_minutes)

    p0 = _nearest_close(df, t0)
    pn = _nearest_close(df, tn)

    if p0 is None or pn is None or p0 == 0.0:
        logger.debug(f"{event.phemex_symbol}: missing price at T0 or T+{delta_minutes}m")
        return None

    delta_pct = round(((pn - p0) / p0) * 100.0, 4)

    return DeltaResult(
        symbol=event.symbol,
        phemex_symbol=event.phemex_symbol,
        announce_ts_ms=event.announce_ts_ms,
        announce_ts_str=event.announce_ts_str,
        price_t0=round(p0, 10),
        price_tn=round(pn, 10),
        delta_pct=delta_pct,
        delta_minutes=delta_minutes,
    )


def filter_by_threshold(
    results: list[DeltaResult],
    min_pct: float,
) -> list[DeltaResult]:
    """Оставляем только |Δ%| >= min_pct, сортируем по убыванию |Δ%|."""
    filtered = [r for r in results if abs(r.delta_pct) >= min_pct]
    filtered.sort(key=lambda r: abs(r.delta_pct), reverse=True)
    return filtered


# ══════════════════════════════════════════════════════════════════
#  Trend (EMA-crossover)
# ══════════════════════════════════════════════════════════════════

class TrendSignal:
    """
    EMA-кросс тренд-детектор.
    fast > slow  → "UP"
    fast <= slow → "DOWN"
    """

    def __init__(self, cfg: BacktestConfig) -> None:
        self._enabled = cfg.trend_enabled
        self._fast = cfg.trend_fast_ema
        self._slow = cfg.trend_slow_ema

    def detect(self, df: pd.DataFrame, symbol: str = "") -> Optional[str]:
        if not self._enabled:
            return "UP"
        if df.empty or "Close" not in df.columns:
            return None
        if len(df) < self._slow:
            logger.debug(f"{symbol}: only {len(df)} bars for trend (need {self._slow})")
            return None

        work = df.copy()
        work["ema_fast"] = work["Close"].ewm(span=self._fast, adjust=False).mean()
        work["ema_slow"] = work["Close"].ewm(span=self._slow, adjust=False).mean()

        last = work.iloc[-1]
        f, s = last["ema_fast"], last["ema_slow"]
        if pd.isna(f) or pd.isna(s):
            return None
        return "UP" if f > s else "DOWN"

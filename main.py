"""
main.py — Оркестратор бэктеста + точка входа.

Пайплайн:
  1. Загружаем инструменты Phemex
  2. Парсим анонсы листингов Upbit
  3. Кросс-фильтр: только монеты с Phemex perpetual
  4. На каждый event: 1m-свечи → compute_delta()
  5. Фильтрация по min_delta_pct
  6. На прошедшие: RSI/trend свечи → финальные метрики
  7. Экспорт JSON
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp

from _math import (
    DeltaResult, ListingEvent, MetricsResult, TrendSignal,
    compute_delta, compute_rsi, filter_by_threshold,
)
from API.phemex import PhemexAdapter
from API.upbit import UpbitParser
from c_log import get_logger
from config import AppConfig, BacktestConfig, PhemexConfig, UpbitConfig
from const import TF_MINUTES

logger = get_logger("main")


# ══════════════════════════════════════════════════════════════════
#  Конфиг — меняй здесь
# ══════════════════════════════════════════════════════════════════

def build_config() -> AppConfig:
    return AppConfig(
        upbit=UpbitConfig(
            # УБИРАЕМ per_page=100, так как Upbit отдает 400 ошибку при значениях > 20
            max_pages=20,
            request_delay_sec=0.4,
        ),
        phemex=PhemexConfig(
            kline_interval_sec=0.15,
            rsi_timeframe="5m",
            rsi_window=14,
            klines_limit=100,
        ),
        backtest=BacktestConfig(
            delta_minutes=15,
            min_delta_pct=3.0,
            trend_enabled=True,
            trend_fast_ema=10,
            trend_slow_ema=30,
        ),
        output_path="results/backtest_results.json",
        log_level="INFO",
    )


# ══════════════════════════════════════════════════════════════════
#  Orchestrator
# ══════════════════════════════════════════════════════════════════

class BacktestEngine:
    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg
        self._upbit   = UpbitParser(cfg.upbit)
        self._phemex  = PhemexAdapter(cfg.phemex)
        self._trend   = TrendSignal(cfg.backtest)

    async def run(self) -> list[MetricsResult]:
        async with aiohttp.ClientSession() as session:
            # ── 1. Phemex instruments ─────────────────────────────────────
            logger.info("━ [1/6] Загружаем инструменты Phemex …")
            await self._phemex.update_instruments(session)
            if not self._phemex.symbols:
                logger.error("Phemex вернул 0 символов — выходим")
                return []

            # ── 2. Upbit listings ─────────────────────────────────────────
            logger.info("━ [2/6] Парсим историю листингов Upbit …")
            # events = await self._upbit.fetch_listings(session)
            # Если хочешь, можешь положить кеш в папку data/ или оставить в корне
            events = await self._upbit.get_cached_listings(session, cache_file="upbit_cache.json")
            if not events:
                logger.warning("Upbit вернул 0 листинговых событий")
                return []

            # ── 3. Кросс-фильтр ──────────────────────────────────────────
            available = [e for e in events if e.phemex_symbol in self._phemex.symbols]
            logger.info(
                f"━ [3/6] {len(available)}/{len(events)} событий имеют "
                "Phemex perpetual"
            )

            # ── 4. Delta per event ────────────────────────────────────────
            logger.info("━ [4/6] Считаем Δ цены …")
            deltas: list[DeltaResult] = []
            for i, ev in enumerate(available, 1):
                logger.info(
                    f"  [{i:>3}/{len(available)}] {ev.phemex_symbol:<16} "
                    f"{ev.announce_ts_str}"
                )
                dr = await self._compute_delta(session, ev)
                if dr is not None:
                    deltas.append(dr)
                await asyncio.sleep(0.05)

            logger.info(f"  → {len(deltas)} результатов дельты")

            # ── 5. Threshold filter ───────────────────────────────────────
            filtered = filter_by_threshold(deltas, self._cfg.backtest.min_delta_pct)
            logger.info(
                f"━ [5/6] {len(filtered)}/{len(deltas)} событий ≥ "
                f"|{self._cfg.backtest.min_delta_pct}%|"
            )

            # ── 6. RSI + trend ────────────────────────────────────────────
            logger.info("━ [6/6] Считаем RSI и тренд …")
            results: list[MetricsResult] = []
            for i, dr in enumerate(filtered, 1):
                sign = "🚀" if dr.delta_pct > 0 else "💥"
                logger.info(
                    f"  [{i:>3}/{len(filtered)}] {dr.phemex_symbol:<16} "
                    f"Δ={dr.delta_pct:+.2f}% {sign}"
                )
                mr = await self._compute_metrics(session, dr, rank=i)
                if mr is not None:
                    results.append(mr)
                await asyncio.sleep(0.05)

            logger.info(f"━ Готово — {len(results)} финальных результатов")
            return results

    # ── Private helpers ───────────────────────────────────────────────────────

    async def _compute_delta(
        self, session: aiohttp.ClientSession, ev: ListingEvent
    ) -> Optional[DeltaResult]:
        t0 = datetime.fromtimestamp(ev.announce_ts_ms / 1000, tz=timezone.utc)
        from_dt = t0 - timedelta(minutes=5)
        to_dt   = t0 + timedelta(minutes=self._cfg.backtest.delta_minutes + 10)

        df = await self._phemex.get_klines_by_time(
            session, ev.phemex_symbol, "1m",
            int(from_dt.timestamp()), int(to_dt.timestamp()),
        )
        return compute_delta(ev, df, self._cfg.backtest.delta_minutes)

    async def _compute_metrics(
        self, session: aiohttp.ClientSession, dr: DeltaResult, rank: int
    ) -> Optional[MetricsResult]:
        tf = self._cfg.phemex.rsi_timeframe
        limit = self._cfg.phemex.klines_limit
        tf_min = TF_MINUTES.get(tf, 1)

        t0 = datetime.fromtimestamp(dr.announce_ts_ms / 1000, tz=timezone.utc)
        from_dt = t0 - timedelta(minutes=tf_min * (limit + 10))
        to_dt   = t0 + timedelta(minutes=tf_min * 3)

        df = await self._phemex.get_klines_by_time(
            session, dr.phemex_symbol, tf,
            int(from_dt.timestamp()), int(to_dt.timestamp()),
        )

        rsi_val: Optional[float] = None
        trend_val: Optional[str] = None

        if not df.empty:
            raw_rsi = compute_rsi(df["Close"], self._cfg.phemex.rsi_window)
            rsi_val = round(raw_rsi, 2) if raw_rsi is not None else None
            trend_val = self._trend.detect(df, dr.phemex_symbol)
        else:
            logger.warning(f"  {dr.phemex_symbol}: нет свечей для RSI/тренда")

        return MetricsResult(
            rank=rank,
            symbol=dr.symbol,
            phemex_symbol=dr.phemex_symbol,
            announce_ts_ms=dr.announce_ts_ms,
            announce_ts_str=dr.announce_ts_str,
            price_t0=dr.price_t0,
            price_tn=dr.price_tn,
            delta_pct=dr.delta_pct,
            delta_minutes=dr.delta_minutes,
            rsi=rsi_val,
            trend=trend_val,
            klines_count=len(df),
        )


# ══════════════════════════════════════════════════════════════════
#  Export
# ══════════════════════════════════════════════════════════════════

def export_results(results: list[MetricsResult], cfg: AppConfig) -> str:
    os.makedirs(os.path.dirname(cfg.output_path) or ".", exist_ok=True)
    now = datetime.now(timezone.utc)
    payload = {
        "meta": {
            "generated_at_utc": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "generated_at_ms":  int(now.timestamp() * 1000),
            "delta_minutes":    cfg.backtest.delta_minutes,
            "min_delta_pct":    cfg.backtest.min_delta_pct,
            "rsi_timeframe":    cfg.phemex.rsi_timeframe,
            "rsi_window":       cfg.phemex.rsi_window,
            "trend_ema":        f"{cfg.backtest.trend_fast_ema}/{cfg.backtest.trend_slow_ema}",
            "total_results":    len(results),
        },
        "results": [r.to_dict() for r in results],
    }
    with open(cfg.output_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    logger.info(f"Экспортировано {len(results)} результатов → {cfg.output_path}")
    return cfg.output_path


# ══════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════

async def main() -> None:
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║   Upbit Listing → Phemex Delta Backtest  ║")
    logger.info("╚══════════════════════════════════════════╝")

    cfg = build_config()
    engine = BacktestEngine(cfg)
    results = await engine.run()

    if not results:
        logger.warning("Нет результатов — выходим.")
        sys.exit(0)

    out_path = export_results(results, cfg)

    # Консольная сводка (топ-15)
    logger.info("")
    logger.info("─── Топ по |Δ%| ─────────────────────────────────────────")
    logger.info(f"{'#':>3}  {'Symbol':>16}  {'Δ%':>8}  {'RSI':>6}  {'Trend':<6}  Время анонса")
    logger.info("─" * 70)
    for r in results[:15]:
        arrow = "↑" if r.delta_pct > 0 else "↓"
        rsi_s = f"{r.rsi:.1f}" if r.rsi is not None else " N/A"
        tr_s  = r.trend or "N/A"
        logger.info(
            f"{r.rank:>3}  {r.phemex_symbol:>16}  "
            f"{r.delta_pct:>+7.2f}%{arrow}  "
            f"{rsi_s:>6}  {tr_s:<6}  {r.announce_ts_str}"
        )
    logger.info("")
    logger.info(f"✓ Полный JSON: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())

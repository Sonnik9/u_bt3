"""
config.py — Конфиги приложения.
Все дефолты здесь, переопределяются в main.py → build_config().
"""
from __future__ import annotations
from dataclasses import dataclass, field

from const import UPBIT_LISTING_KEYWORDS


@dataclass
class UpbitConfig:
    notice_url: str = "https://upbit.com/api/v1/notices"
    per_page: int = 100
    max_pages: int = 20
    request_delay_sec: float = 0.4
    listing_keywords: list[str] = field(default_factory=lambda: UPBIT_LISTING_KEYWORDS)


@dataclass
class PhemexConfig:
    base_url: str = "https://api.phemex.com"
    kline_interval_sec: float = 0.15   # rate-limit guard
    rsi_timeframe: str = "5m"
    rsi_window: int = 14
    klines_limit: int = 100


@dataclass
class BacktestConfig:
    delta_minutes: int = 15       # T+N минут для замера Δ цены (1–60)
    min_delta_pct: float = 3.0    # минимальный |Δ%| для попадания в результат
    trend_enabled: bool = True
    trend_fast_ema: int = 10
    trend_slow_ema: int = 30


@dataclass
class AppConfig:
    upbit: UpbitConfig = field(default_factory=UpbitConfig)
    phemex: PhemexConfig = field(default_factory=PhemexConfig)
    backtest: BacktestConfig = field(default_factory=BacktestConfig)
    output_path: str = "results/backtest_results.json"
    log_level: str = "INFO"

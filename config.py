from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class UpbitConfig:
    # /announcements — НЕ /notices. Это и была корневая ошибка.
    notice_url: str = "https://api-manager.upbit.com/api/v1/announcements"
    per_page: int = 20          # столько же сколько в рабочем DEFAULT_CONFIG
    max_pages: int = 50         # столько же
    request_delay_sec: float = 0.2
    listing_keywords: list[str] = field(default_factory=lambda: [
        "Market Support for",   # EN-заголовки
        "신규 거래지원",          # KR: новая поддержка торговли
        "디지털 자산 추가",        # KR: добавление цифрового актива
    ])


@dataclass
class PhemexConfig:
    base_url: str = "https://api.phemex.com"
    kline_interval_sec: float = 0.15
    rsi_timeframe: str = "5m"
    rsi_window: int = 14
    klines_limit: int = 100


@dataclass
class BacktestConfig:
    delta_minutes: int = 15
    min_delta_pct: float = 3.0
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

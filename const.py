"""
const.py — Единственное место для всех констант проекта.
"""
from __future__ import annotations
from typing import Any

# ── Phemex kline resolutions ────────────────────────────────────────────────
RESOLUTION_MAP: dict[str, int] = {
    "1m": 60, "5m": 300, "15m": 900,
    "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400,
}
TF_MINUTES: dict[str, int] = {
    "1m": 1, "5m": 5, "15m": 15,
    "30m": 30, "1h": 60, "4h": 240, "1d": 1440,
}
PHEMEX_ALLOWED_LIMITS: list[int] = [5, 10, 50, 100, 500, 1000]

# ── Upbit notice API ────────────────────────────────────────────────────────
UPBIT_NOTICE_URL: str = "https://upbit.com/api/v1/notices"

# Ключевые слова листинга на корейском (проверяется по title)
UPBIT_LISTING_KEYWORDS: list[str] = [
    "신규 거래지원",   # New trading support  ← ГЛАВНОЕ ключевое слово
    "마켓 추가",       # Market added
    "디지털 자산 추가",# Digital asset added
    "신규 상장",       # New listing
    "상장 안내",       # Listing guide
]

# Символы-обманки которые не являются тикерами монет
SYMBOL_BLACKLIST: frozenset[str] = frozenset({
    "KRW", "BTC", "ETH", "USDT", "THE", "NEW", "FOR", "AND",
    "ALL", "CEO", "ICO", "API", "FAQ", "URL", "USD", "EUR",
    "NFT", "DEFI", "DAO", "IPO", "DEX", "CEX", "ATH",
    "KRW", "USDT", "BTC", "USDT", "ERC",
})

# ── Logging ─────────────────────────────────────────────────────────────────
LOG_FORMAT: str = "%(asctime)s  %(name)-20s  %(levelname)-8s  %(message)s"
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"

# ── Trend pattern (used by TrendSignal) ─────────────────────────────────────
TREND_PATTERN: dict[str, Any] = {
    "5m": {"enable": True, "fast": 10, "slow": 30},
}

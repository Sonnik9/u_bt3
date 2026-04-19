"""
c_log.py — Простой логгер с файловым ротатором.
"""
from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from const import LOG_DATE_FORMAT, LOG_FORMAT


def get_logger(name: str, level: str = "INFO", log_dir: str = "logs") -> logging.Logger:
    """
    Возвращает настроенный logger.
    Пишет в stdout + ротируемый файл logs/<name>.log
    """
    log = logging.getLogger(name)
    if log.handlers:
        return log

    log.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # stdout
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)

    # файл
    try:
        os.makedirs(log_dir, exist_ok=True)
        fh = RotatingFileHandler(
            f"{log_dir}/{name}.log",
            maxBytes=2 * 1024 * 1024,  # 2 MB
            backupCount=3,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        log.addHandler(fh)
    except Exception:
        pass  # не критично — работаем только через stdout

    log.propagate = False
    return log


# Совместимость со старым кодом: UnifiedLogger как alias
class UnifiedLogger:
    def __init__(self, name: str, level: str = "INFO") -> None:
        self._log = get_logger(name, level)

    def debug(self, msg: str) -> None:    self._log.debug(msg)
    def info(self, msg: str) -> None:     self._log.info(msg)
    def warning(self, msg: str) -> None:  self._log.warning(msg)
    def error(self, msg: str) -> None:    self._log.error(msg)
    def exception(self, msg: str) -> None: self._log.exception(msg)
    def set_level(self, level: str) -> None:
        self._log.setLevel(getattr(logging, level.upper(), logging.INFO))

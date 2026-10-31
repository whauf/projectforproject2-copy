"""Task loggers: every message goes to the Prefect run logger and to logs/<task>.log."""

import logging

from prefect import get_run_logger

from .config import LOG_DIR

_FORMAT = "%(asctime)s | %(levelname)-7s | %(message)s"


def _file_logger(name: str) -> logging.Logger:
    """A plain logger that appends to logs/<name>.log (created once per process)."""
    logger = logging.getLogger(f"package_health.{name}")
    if not logger.handlers:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(LOG_DIR / f"{name}.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter(_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


class TaskLogger:
    """Tee a task's messages to Prefect (console + UI) and to the task's own log file."""

    def __init__(self, name: str):
        self.prefect = get_run_logger()
        self.file = _file_logger(name)

    def log(self, level: int, msg: str, *args) -> None:
        self.prefect.log(level, msg, *args)
        self.file.log(level, msg, *args)

    def info(self, msg: str, *args) -> None:
        self.log(logging.INFO, msg, *args)

    def warning(self, msg: str, *args) -> None:
        self.log(logging.WARNING, msg, *args)

    def error(self, msg: str, *args) -> None:
        self.log(logging.ERROR, msg, *args)

    def table(self, title: str, frame) -> None:
        """Log a pandas DataFrame as an aligned text table."""
        self.info("%s\n%s", title, frame.to_string(index=False))

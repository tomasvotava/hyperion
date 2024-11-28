import logging as _logging
import sys
import time
import traceback
from collections.abc import Callable
from types import TracebackType
from typing import Any, ParamSpec, TypeVar

import loguru

from hyperion.config import config

P = ParamSpec("P")
T = TypeVar("T")

LOGURU_PRETTY_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>\n<dim>{extra}</dim>"
)


def setup_logger_from_env() -> None:
    loguru.logger.remove()
    if config.log_pretty:
        loguru.logger.add(sys.stderr, colorize=True, format=LOGURU_PRETTY_FORMAT, level=config.log_level)
    else:
        loguru.logger.add(sys.stderr, format="{message}", serialize=True, level=config.log_level)


class InterceptHandler(_logging.Handler):
    def emit(self, record: _logging.LogRecord) -> None:
        log_level: str | int
        try:
            log_level = loguru.logger.level(record.levelname).name
        except ValueError:
            log_level = record.levelno
        frame = _logging.currentframe()
        depth = 2
        while frame.f_code.co_filename == _logging.__file__:
            if frame.f_back is None:
                break
            frame = frame.f_back
            depth += 1
        loguru.logger.opt(depth=depth, exception=record.exc_info).log(log_level, record.getMessage())


def intercept_python_loggers() -> None:
    _logging.root.handlers = []
    _logging.root.setLevel(_logging.DEBUG)
    _logging.root.addHandler(InterceptHandler())


class LogTiming:
    def __init__(self, action: str | None = None, logger: "loguru.Logger | None" = None) -> None:
        self.action = action
        self.start_time: float = 0
        self.logger = logger or get_logger("timing")

    def __enter__(self) -> None:
        self.start_time = time.monotonic()

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: TracebackType | None = None,
    ) -> None:
        duration = time.monotonic() - self.start_time
        action = self.action or "unnamed action"
        if exc:
            self.logger.warning(
                f"Action {action!r} failed after {duration:0.3f} seconds.",
                action=self.action,
                duration=duration,
                exception_type=exc_type.__name__ if exc_type else "unknown",
                exception=str(exc),
                traceback="\n".join(traceback.format_tb(tb)),
            )
            return
        self.logger.info(
            f"Action {action!r} finished after {duration:0.3f} seconds.", action=self.action, duration=duration
        )

    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        if self.action is None:
            self.action = func.__name__

        def _wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            with self:
                return func(*args, **kwargs)

        return _wrapped


setup_logger_from_env()
intercept_python_loggers()


def get_logger(service: str, **context: Any) -> "loguru.Logger":
    """Get logger bound to a service with optional contextual variables."""
    return loguru.logger.bind(service=service, **context)


if __name__ == "__main__":
    demo_logger = get_logger("demo")
    demo_logger.trace("Trace message")
    demo_logger.debug("Debug message")
    demo_logger.info("Informational message")
    demo_logger.warning("A warning!")
    demo_logger.error("An error...")
    demo_logger.critical("This is terrible.")
    with LogTiming("This will take around 1 second.", demo_logger):
        time.sleep(1)

    try:
        with LogTiming("This will fail after around 1 second.", demo_logger):
            time.sleep(1)
            raise ValueError("This failed.")
    except Exception as error:
        demo_logger.exception(error)

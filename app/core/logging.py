import logging
from pathlib import Path
from rich.logging import RichHandler
from app.core.config import get_settings

settings = get_settings()
log_level = getattr(logging, settings.log_level.upper())
log_folder = settings.log_folder

app_logger = logging.getLogger("app")
app_logger.setLevel(log_level)
app_logger.addHandler(RichHandler(rich_tracebacks=True))
app_logger.propagate = False


def get_logger(name: str, log_file: Path | None = None) -> logging.Logger:
    full_name = f"app.{name}" if not name.startswith("app.") else name
    logger = logging.getLogger(full_name)

    if log_file:
        file_handler = logging.FileHandler(log_folder / log_file)
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger

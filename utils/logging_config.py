import logging
import sys
from pathlib import Path


LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline.log"
MONITORING_LOG_FILE = LOG_DIR / "monitoring.log"

logger = logging.getLogger("__name__")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

if not logger.handlers:
    # Console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Main Log File
    file_handler = logging.FileHandler(LOG_FILE, mode="a")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Monitoring Log File
    monitoring_file_handler = logging.FileHandler(MONITORING_LOG_FILE, mode="a")
    monitoring_file_handler.setLevel(logging.ERROR)
    monitoring_file_handler.setFormatter(formatter)
    logger.addHandler(monitoring_file_handler)

# Attach logging level constants to the logger object for convenience

logger.DEBUG = logging.DEBUG             # type: ignore[attr-defined]
logger.INFO = logging.INFO               # type: ignore[attr-defined]
logger.WARNING = logging.WARNING         # type: ignore[attr-defined]
logger.ERROR = logging.ERROR             # type: ignore[attr-defined]
logger.CRITICAL = logging.CRITICAL       # type: ignore[attr-defined]

__all__ = ["logger"]
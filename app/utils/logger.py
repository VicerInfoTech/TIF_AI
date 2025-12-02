# Structured logging
import logging
import os
import time
from datetime import datetime

# --- Setup Log Directory ---
from pathlib import Path

# Compute project root locally to avoid import cycles with `app.config`
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = os.path.join(str(PROJECT_ROOT), "Log")
os.makedirs(LOG_DIR, exist_ok=True)


def get_daily_log_path() -> str:
    """Generate a log file path with the current date"""
    today = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(LOG_DIR, f"app_{today}.log")


def setup_logging(name: str = __name__, level: int | str | None = None) -> logging.Logger:
    """Setup structured logging with daily rotation and noise suppression.

    Args:
        name: The module name to attach to the logger.
        level: Optional logging level (int or string like 'INFO').
            If None, uses the LOG_LEVEL environment variable or defaults to DEBUG.
    """
    log_path = get_daily_log_path()

    # Create a project-specific logger
    logger = logging.getLogger(name)
    # Resolve configured log level (function arg > env var > default)
    if level is None:
        env_level = os.getenv("ENV_LOG_LEVEL")
        level = env_level if env_level else logging.DEBUG
    # Accept strings like 'INFO' or numeric levels
    try:
        resolved_level = logging.getLevelName(level) if isinstance(level, str) else int(level)
        # logging.getLevelName returns an int when given a string like 'INFO'
        if isinstance(resolved_level, str):
            # If getLevelName returns the name for an int, fallback to DEBUG
            resolved_level = logging.DEBUG
    except Exception:
        resolved_level = logging.DEBUG
    logger.setLevel(resolved_level)
    logger.propagate = False  # prevent bubbling to root logger

    # Clear old handlers
    if logger.handlers:
        for h in logger.handlers[:]:
            logger.removeHandler(h)

    # File handler
    try:
        file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(module)s:%(lineno)d - %(levelname)s - %(message)s",
            "%Y-%m-%d %H:%M:%S"
        ))
        # Set handler levels to match requested overall level
        file_handler.setLevel(resolved_level)
        logger.addHandler(file_handler)
    except Exception:
        pass

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(module)s:%(lineno)d - %(levelname)s - %(message)s",
        "%Y-%m-%d %H:%M:%S"
    ))
    console_handler.setLevel(resolved_level)
    logger.addHandler(console_handler)

    # Suppress noisy third-party logs globally
    noisy_libs = [
        "asyncio", "urllib3", "matplotlib", "PIL",
        "tensorflow", "torch", "numba", "ultralytics", "cv2",
    ]
    for lib in noisy_libs:
        logging.getLogger(lib).setLevel(logging.WARNING)

    return logger



def cleanup_old_logs(days_to_keep: int = 30):
    """Delete log files older than specified days."""
    now = time.time()
    for filename in os.listdir(LOG_DIR):
        if filename.startswith("app_") and filename.endswith(".log"):
            file_path = os.path.join(LOG_DIR, filename)
            try:
                if os.path.getmtime(file_path) < now - (days_to_keep * 86400):
                    os.remove(file_path)
                    logging.info(f"Deleted old log file: {filename}")
            except Exception:
                pass


# --- Example Usage ---
if __name__ == "__main__":
    logger = setup_logging(__name__, level="DEBUG")
    logger.debug("Debug log test — only visible if LOG_LEVEL=DEBUG.")
    logger.info("App started successfully.")
    cleanup_old_logs(30)

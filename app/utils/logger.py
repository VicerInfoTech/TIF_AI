# Structured logging
import logging
import os
import time
from datetime import datetime

# --- Setup Log Directory ---
PROJECT_ROOT = "D:\\sql-insight-agent"
LOG_DIR = os.path.join(PROJECT_ROOT, "Log")
os.makedirs(LOG_DIR, exist_ok=True)


def get_daily_log_path() -> str:
    """Generate a log file path with the current date"""
    today = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(LOG_DIR, f"app_{today}.log")


def setup_logging(name: str = __name__) -> logging.Logger:
    """Setup structured logging with daily rotation and noise suppression."""
    log_path = get_daily_log_path()

    # Dynamic log level from environment or fallback
    # level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    # level_name = "DEBUG"
    # log_level = getattr(logging, level_name, logging.INFO)

    # Clear existing handlers to avoid duplicate logs
    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    # Handlers: File + Console
    handlers = [logging.StreamHandler()]
    try:
        handlers.insert(0, logging.FileHandler(log_path, mode="a", encoding="utf-8"))
    except Exception:
        # If file handler fails, fallback to console only
        pass

    # Configure base logger
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(module)s:%(lineno)d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )

    # Suppress noisy third-party logs
    noisy_libs = [
        "asyncio", "urllib3", "matplotlib", "PIL",
        "tensorflow", "torch", "numba", "ultralytics", "cv2",
    ]
    for lib in noisy_libs:
        logging.getLogger(lib).setLevel(logging.WARNING)

    # Return a named logger for the caller module
    return logging.getLogger(name)


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
    logger = setup_logging(__name__)
    logger.debug("Debug log test — only visible if LOG_LEVEL=DEBUG.")
    logger.info("App started successfully.")
    cleanup_old_logs(30)

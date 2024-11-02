import logging
import logging.config


def setup_logging():
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {"format": "%(asctime)s - %(levelname)s - %(message)s"},
        },
        "handlers": {
            "console": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "standard",
            },
        },
        "root": {
            "handlers": ["console"],
            "level": "INFO",
        },
    }

    logging.config.dictConfig(logging_config)

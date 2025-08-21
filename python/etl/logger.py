from loguru import logger
import os

LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger.remove()
logger.add(lambda msg: print(msg, end=""), level=LEVEL)

import logging


# logger.py


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s %(message)s",
    datefmt="%d.%m.%Y %H:%M:%S"
)

logger = logging.getLogger("fabricengineer")

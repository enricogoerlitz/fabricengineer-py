import logging


# logger.py


logging.getLogger("py4j").propagate = False

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s %(message)s",
    datefmt="%d.%m.%Y %H:%M:%S"
)

logger = logging.getLogger("fabricengineer")

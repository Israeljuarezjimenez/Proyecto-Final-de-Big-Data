import logging
import sys

def configurar_logging(nombre=None, nivel="INFO"):
    logger = logging.getLogger(nombre if nombre else __name__)
    if logger.handlers:
        return logger

    logger.setLevel(nivel.upper())
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

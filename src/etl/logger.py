import logging

def get_logger(name: str = "etl"):
    """
    Логирование... как в лучших домах Франции
    Делаем всё чин-чинарём с типизацией и логами, CookieCutter, форматированием в black и PEP стандартом
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
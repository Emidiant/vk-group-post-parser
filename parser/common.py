import logging
import colorlog

def get_logger(class_name, log_level=logging.DEBUG):
    logger = logging.Logger(name=class_name, level=log_level)
    stream_handler = colorlog.StreamHandler()
    stream_handler.setFormatter(
        colorlog.ColoredFormatter('%(log_color)s%(asctime)s  %(name)16s  %(levelname)8s: %(message)s'))
    logger.addHandler(stream_handler)
    return logger
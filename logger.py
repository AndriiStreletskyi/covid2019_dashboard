import logging

logging.basicConfig(filename="board.log", level=logging.DEBUG, format=f"%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s")
logger = logging.getLogger(__name__)

def func_logger(func):

    def log_write(*args,**kwargs):
        ret = func(*args,**kwargs)
        logger.info(f"Call func {func.__name__} with {args, kwargs} returns {ret}")
        return ret
    
    return log_write

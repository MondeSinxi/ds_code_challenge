import logging
from functools import wraps
from time import time

def timing(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        ts = time()
        result = f(*args, **kwargs)
        te = time()
        logging.info(f"Function: {f.__name__} with args: {args} and kwargs: {kwargs} took {te - ts} seconds")
        return result
    return wrap

import logging
from functools import wraps
import random
from time import time


def timing(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        ts = time()
        result = f(*args, **kwargs)
        te = time()
        logging.info(
            f"Function: {f.__name__} with args: {args} and kwargs: {kwargs} took {te - ts} seconds"
        )
        return result

    return wrap


def generate_array_of_randoms(n=10):
    return [random.random() for _ in range(n)]

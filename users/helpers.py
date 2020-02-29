from time import time


def timing(f):
    """decorator to measure the execution time of methods"""
    def wrapper(*args, **kwargs):
        start = time()
        result = f(*args, **kwargs)
        end = time()
        print('Time: {}'.format(end-start))
        return result
    return wrapper
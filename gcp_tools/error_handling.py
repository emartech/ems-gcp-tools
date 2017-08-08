import logging
import time
from functools import wraps


def retry_on_error(number_of_retries=5, init_wait=5):
    def decorator(fun):
        @wraps(fun)
        def wrapped(*args, **kwargs):
            logger = logging.getLogger(fun.__name__)

            wait0 = init_wait
            wait1 = init_wait
            attempts = 0
            while attempts <= number_of_retries:
                try:
                    return fun(*args, **kwargs)
                except Exception as e:
                    if attempts < number_of_retries:
                        attempts += 1
                        logger.exception('Error encountered. Retry {}/{} in {} s.'.format(attempts, number_of_retries, wait1))
                        time.sleep(wait1)
                        wait0, wait1 = wait1, wait0+wait1
                    else:
                        logger.exception('Error encountered after {} retries. Raising error.'.format(number_of_retries))
                        raise e
        return wrapped
    return decorator
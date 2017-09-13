import logging
import time
from functools import wraps


class DefaultWaitingStrategy():

    def __init__(self, init_wait=5):
        self._previous = 0
        self._current = init_wait

    def next(self, attempts):
        self._current, self._previous = self._current + self._previous, self._current

        return self._current


class FixedWaitingStrategy():

    def __init__(self, init_wait=5, seconds=5):
        self._init_wait = init_wait
        self._seconds = seconds

    def next(self, attempts):
        if 1 == attempts:
            return self._init_wait
        else:
            return self._seconds


class LinearWaitingStrategy():

    def __init__(self, init_wait=5, increment=5):
        self._init_wait = init_wait
        self._increment = increment

    def next(self, attempts):
        return self._init_wait + (attempts - 1) * self._increment


def retry_on_error(number_of_retries=5, init_wait=5, waiting_strategy=None):
    def decorator(fun):
        @wraps(fun)
        def wrapped(*args, **kwargs):
            if waiting_strategy is None:
                _waiting_strategy = DefaultWaitingStrategy(init_wait)
            else:
                _waiting_strategy = waiting_strategy

            logger = logging.getLogger(fun.__name__)

            elapsed_seconds = 0
            attempts = 0
            while attempts <= number_of_retries:
                try:
                    return fun(*args, **kwargs)
                except Exception as e:
                    if attempts < number_of_retries:
                        attempts += 1
                        sleep_seconds = _waiting_strategy.next(attempts)
                        elapsed_seconds += sleep_seconds
                        logger.exception('Error encountered. Retry {}/{} in {} s.'.format(attempts, number_of_retries, elapsed_seconds))
                        time.sleep(sleep_seconds)
                    else:
                        logger.exception('Error encountered after {} retries. Raising error.'.format(number_of_retries))
                        raise e
        return wrapped
    return decorator
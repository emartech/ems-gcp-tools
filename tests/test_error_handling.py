import pytest
from gcp_tools.error_handling import retry_on_error, DefaultWaitingStrategy, LinearWaitingStrategy, FixedWaitingStrategy


def test_retry_on_error_functionWithoutError_calledOnlyOnce():
    class RetryOnErrorTestClass():
        def __init__(self):
            self.num_fun_calls = 0

        @retry_on_error()
        def return_without_error(self):
            self.num_fun_calls += 1
            return self.num_fun_calls

    test_case = RetryOnErrorTestClass()
    assert test_case.return_without_error() == 1


def test_retry_on_error_functionWithSingleError_calledOnlyTwice():
    class RetryOnErrorTestClass():
        def __init__(self):
            self.num_fun_calls = 0

        @retry_on_error(init_wait=0)
        def return_without_error(self):
            self.num_fun_calls += 1
            if self.num_fun_calls == 1:
                raise Exception('blabla')
            else:
                return self.num_fun_calls

    test_case = RetryOnErrorTestClass()
    assert test_case.return_without_error() == 2


def test_retry_on_error_functionWithAlwaysError_calledUntilNumRetriesReachedThenExceptionRaised():
    class RetryOnErrorTestClass():
        def __init__(self):
            self.num_fun_calls = 0

        @retry_on_error(number_of_retries=3, init_wait=0)
        def return_without_error(self):
            self.num_fun_calls += 1
            raise Exception('blabla')

    test_case = RetryOnErrorTestClass()
    with pytest.raises(Exception):
        test_case.return_without_error()

    assert test_case.num_fun_calls == 4


def test_constructor_defaultWaitingStrategy():
    strategy = DefaultWaitingStrategy(2)

    assert 2 == strategy.next(1)
    assert 4 == strategy.next(2)
    assert 6 == strategy.next(3)
    assert 10 == strategy.next(4)


def test_constructor_fixedWaitingStrategy():
    strategy = FixedWaitingStrategy(3, 7)

    assert 3 == strategy.next(1)
    assert 7 == strategy.next(2)
    assert 7 == strategy.next(3)


def test_constructor_linearWaitingStrategy():
    strategy = LinearWaitingStrategy(3, 7)

    assert 3 == strategy.next(1)
    assert 10 == strategy.next(2)
    assert 17 == strategy.next(3)

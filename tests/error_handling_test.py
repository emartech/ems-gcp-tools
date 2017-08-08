import pytest
from gcp_tools.error_handling import retry_on_error


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

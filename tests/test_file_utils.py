import pytest

from gcp_tools.file_utils import strip_extension

extensions_to_strip = ['gz', 'csv', '.csv.gz']

extension_tests = [
    ('asdasdsad.csv.gz', extensions_to_strip, 'asdasdsad'),
    ('asdasdsad.csv', extensions_to_strip, 'asdasdsad'),
    ('asdasdsad.gz', extensions_to_strip, 'asdasdsad'),
    ('asdasdsad.txt.gz', extensions_to_strip, 'asdasdsad.txt'),
    ('asdas.dsad.csv.gz', extensions_to_strip, 'asdas.dsad'),
    ('asdasdsad', extensions_to_strip, 'asdasdsad'),
    ('asdasdsad.gz.txt', extensions_to_strip, 'asdasdsad.gz.txt')
]


@pytest.mark.parametrize('input,extensions,expected_output', extension_tests)
def test_strip_extension_returnsExpectedOutput(input, extensions, expected_output):
    stripped_filename = strip_extension(input, extensions)
    assert stripped_filename == expected_output
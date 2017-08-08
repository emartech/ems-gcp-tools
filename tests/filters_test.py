import pytest

from gcp_tools.filters import filter_table_name, filter_table_name_by_shard_date

table_names_for_test_filter_table_name = [
    ('ssdfsd', 'sends_.*', True, False),
    ('sends', 'sends_.*', True, False),
    ('sends_11', 'sends_.*', True, False),
    ('sends_11', 'sends_.*', False, True),
    ('sends_20160101', 'sends_.*', True, True),
    ('sends_20160199', 'sends_.*', True, False),
    ('sends_201611', 'sends_.*', True, False),
    ('sends_2016111', 'sends_.*', True, False)
]


@pytest.mark.parametrize('bq_table_name,bq_table_pattern,only_date_shards,expected_output',
                         table_names_for_test_filter_table_name)
def test_filter_table_name(bq_table_name, bq_table_pattern, only_date_shards, expected_output):
    output = filter_table_name(bq_table_name, bq_table_pattern, only_date_shards)
    assert output == expected_output


table_names_for_test_filter_table_name_by_shard_name = [
    ('sends_20160101', '20160101', '20160102', True),
    ('sends_20160101', '20160102', '20160102', False),
    ('sends_20160102', '20160101', '20160102', True),
    ('sends_20160102', '20160101', '20160101', False),
    ('sends_20160101', '20160101', '20160101', True),
    ('sends_2016010', '20160101', '20160101', False),
    ('sendsabc', '20160101', '20160101', False)
]


@pytest.mark.parametrize('bq_table_name,min_shard_date,max_shard_date,expected_output',
                         table_names_for_test_filter_table_name_by_shard_name)
def test_filter_table_name_by_shard_date(bq_table_name, min_shard_date, max_shard_date, expected_output):
    output = filter_table_name_by_shard_date(bq_table_name, min_shard_date, max_shard_date)
    assert output == expected_output

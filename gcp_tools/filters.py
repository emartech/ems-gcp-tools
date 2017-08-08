import re
from datetime import datetime


def filter_table_name(bq_table_name, bq_table_pattern, only_date_shards):
    pattern_matches = re.match(bq_table_pattern, bq_table_name) is not None

    if only_date_shards:
        try:
            shard_date_str = re.match('.*?_(\d+)$', bq_table_name).groups()[0]
            shard_date = datetime.strptime(shard_date_str, '%Y%m%d')
            shard_check_ok = datetime.strftime(shard_date, '%Y%m%d') == shard_date_str
        except (AttributeError, ValueError) as e:
            shard_check_ok = False

        return pattern_matches and shard_check_ok
    else:
        return pattern_matches


def filter_table_name_by_shard_date(bq_table_name, date_include_min, date_include_max):
    try:
        shard_date_str = re.match('.*?_(\d+)$', bq_table_name).groups()[0]
    except AttributeError as e:
        return False

    try:
        shard_date = datetime.strptime(shard_date_str, '%Y%m%d')
    except ValueError as e:
        return False

    return datetime.strptime(date_include_min, '%Y%m%d') <= shard_date <= datetime.strptime(date_include_max, '%Y%m%d')
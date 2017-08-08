from gcp_tools.bq_tools import schema_dict_creator, get_table_schema_from_json


def test_schema_dict_creator():
    schema = get_table_schema_from_json('resources/test_schema.json')
    assert len(schema_dict_creator(schema)['record'][2]) == 2

from gcp_tools.bq_tools import schema_dict_creator, get_table_schema_from_json

SOURCE_JSON_PATH = "resources/test_schema.json"


def test_schema_dict_creator_createsDictCorrectly():
    schema = get_table_schema_from_json(SOURCE_JSON_PATH)
    assert len(schema_dict_creator(schema)["record"][2]) == 2


def test_get_table_schema_from_json_createsNestedSchemaList():
    schema = get_table_schema_from_json(SOURCE_JSON_PATH)
    assert len(schema) == 3
    assert schema[1].field_type == "RECORD"
    assert len(schema[1].fields) == 2

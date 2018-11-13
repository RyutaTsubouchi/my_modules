import unittest
import dynamodblocal_yaml
from dynamodblocal_yaml import dynamodb


class TestCase(unittest.TestCase):
    def test_setup(self):
        dynamodblocal_yaml.create_tables(dynamodb,"myTableName")
        response = dynamodb.Table('myTableName').table_status
        expected = "ACTIVE"
        self.assertEqual(response, expected)
        
        print(dynamodb.Table('myTableName').attribute_definitions)
        print(dynamodb.Table('myTableName').creation_date_time)
        print(dynamodb.Table('myTableName').global_secondary_indexes)
        print(dynamodb.Table('myTableName').item_count)
        print(dynamodb.Table('myTableName').key_schema)
        print(dynamodb.Table('myTableName').latest_stream_arn)
        print(dynamodb.Table('myTableName').latest_stream_label)
        print(dynamodb.Table('myTableName').local_secondary_indexes)
        print(dynamodb.Table('myTableName').provisioned_throughput)
        print(dynamodb.Table('myTableName').restore_summary)
        print(dynamodb.Table('myTableName').sse_description)
        print(dynamodb.Table('myTableName').stream_specification)
        print(dynamodb.Table('myTableName').table_arn)
        print(dynamodb.Table('myTableName').table_id)
        print(dynamodb.Table('myTableName').table_name)
        print(dynamodb.Table('myTableName').table_size_bytes)
        print(dynamodb.Table('myTableName').table_status)

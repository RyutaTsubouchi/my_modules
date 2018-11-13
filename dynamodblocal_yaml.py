import boto3
import yaml

dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
f = open("sample.yml")
data = yaml.load(f)
names = []
for key, value in data['Resources'].items():
    names.append(value['Properties']['TableName'])


def create_table(dynamodb, table_name):
    if table_name in names:
        for key, value in data['Resources'].items():
            if value['Properties']['TableName'] == table_name:
                params = value['Properties']

                params['ProvisionedThroughput']['ReadCapacityUnits'] = int(
                    params['ProvisionedThroughput']['ReadCapacityUnits'])
                params['ProvisionedThroughput']['WriteCapacityUnits'] = int(
                    params['ProvisionedThroughput']['WriteCapacityUnits'])
                if 'GlobalSecondaryIndexes' in params:
                    for l in params['GlobalSecondaryIndexes']:
                        l['ProvisionedThroughput']['ReadCapacityUnits'] = int(
                            l['ProvisionedThroughput']['ReadCapacityUnits'])
                        l['ProvisionedThroughput']['WriteCapacityUnits'] = int(
                            l['ProvisionedThroughput']['WriteCapacityUnits'])

        return dynamodb.create_table(**params)
    else:
        print('失敗')


def create_all_tables(dynamodb):
    for key, value in data['Resources'].items():
        if value['Type'] == 'AWS::DynamoDB::Table':
            params = value['Properties']

            params['ProvisionedThroughput']['ReadCapacityUnits'] = int(
                params['ProvisionedThroughput']['ReadCapacityUnits'])
            params['ProvisionedThroughput']['WriteCapacityUnits'] = int(
                params['ProvisionedThroughput']['WriteCapacityUnits'])
            if 'GlobalSecondaryIndexes' in params:
                for l in params['GlobalSecondaryIndexes']:
                    l['ProvisionedThroughput']['ReadCapacityUnits'] = int(
                        l['ProvisionedThroughput']['ReadCapacityUnits'])
                    l['ProvisionedThroughput']['WriteCapacityUnits'] = int(
                        l['ProvisionedThroughput']['WriteCapacityUnits'])
    return dynamodb.create_table(**params)


def create_tables(dynamodb, *args):
    for table_name in args:
        delete_table(dynamodb, table_name)
        create_table(dynamodb, table_name)


def delete_table(dynamodb, table_name):
    table = dynamodb.Table(table_name)
    try:
        table.delete()
    except:
        pass


def delete_tables(dynamodb, *args):
    for table_name in args:
        delete_table(dynamodb, table_name)

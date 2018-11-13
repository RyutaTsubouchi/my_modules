'''
    【前提】
    ・dynamodblocal_yamlをインポートしている
    ・変数dynamodbをインポートしている(from dynamodblocal_yaml import dynamodb)
    ・testディレクトリ下に読み込み対象のyamlファイルが存在する

    【使い方】
    create_tables(dynamodb,テーブル名(リスト可))：yamlにあるテーブル名を指定してテーブルを作成(同名テーブルが存在する場合、削除後に再作成)
    create_all_tables(dynamodb)：yamlにあるすべてのdynamodbテーブルを作成(全てのテーブルを削除後に再作成)
    delete_tables(dynamodb,テーブル名)：yamlにあるテーブル名を指定してテーブル削除

    【リストから複数のテーブルを作成する例】
    tables = ['example1', 'example2', 'example3']
    create_tables(dynamodb, *tables)
'''
import boto3
import yaml

names = []
f = open('sample.yml')
data = yaml.load(f)
for key, value in data['Resources'].items():
    names.append(value['Properties']['TableName'])

dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')


def create_table(dynamodb, table_name):
    if table_name in names:
        for key, value in data['Resources'].items():
            if value['Type'] == 'AWS::DynamoDB::Table':
                if value['Properties']['TableName'] == table_name:
                    params = value['Properties']

                    params['ProvisionedThroughput']['ReadCapacityUnits'] = int(
                        params['ProvisionedThroughput']['ReadCapacityUnits'])
                    params['ProvisionedThroughput']['WriteCapacityUnits'] = int(
                        params['ProvisionedThroughput']['WriteCapacityUnits'])
                    if 'GlobalSecondaryIndexes' in params:
                        for l in params['GlobalSecondaryIndexes']:
                            l['ProvisionedThroughput'][
                                'ReadCapacityUnits'] = int(
                                    l['ProvisionedThroughput']
                                    ['ReadCapacityUnits'])
                            l['ProvisionedThroughput'][
                                'WriteCapacityUnits'] = int(
                                    l['ProvisionedThroughput']
                                    ['WriteCapacityUnits'])

        return dynamodb.create_table(**params)
    else:
        return None


def create_tables(dynamodb, *args):
    for table_name in args:
        delete_table(dynamodb, table_name)
        create_table(dynamodb, table_name)


def create_all_tables(dynamodb):
    for table_name in names:
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


def delete_all_tables(dynamodb):
    for table_name in names:
        table = dynamodb.Table(table_name)
        try:
            table.delete()
        except:
            pass

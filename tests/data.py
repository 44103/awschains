import json
import os
import boto3


AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
SAMPLE_TABLE = os.environ["SAMPLE_TABLE"]


def setup_dynamodb_table():
    """DynamoDBテーブル生成"""

    dynamodb = boto3.resource("dynamodb")
    mock_table = dynamodb.create_table(
        TableName=SAMPLE_TABLE,
        KeySchema=[
            {"AttributeName": "ForumName", "KeyType": "HASH"},
            {"AttributeName": "Subject", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "ForumName", "AttributeType": "S"},
            {"AttributeName": "Subject", "AttributeType": "S"},
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1,
        },
    )
    return mock_table


def read_json(file_path: str, float_as=float):
    """テストデータ読み込み"""

    with open(f"{file_path}.json") as f:
        item = json.load(f, parse_float=float_as)
    return item

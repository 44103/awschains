from decimal import Decimal
from typing import Type
from moto import mock_dynamodb
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from pytest import FixtureRequest, MonkeyPatch, fixture, raises
import sys

sys.path.append("../")

from src.awschains.dynamochain import DynamoChain


@fixture(autouse=True)
def init_modules(request: Type[FixtureRequest], monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "dummy")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "dummy")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-1")
    monkeypatch.setenv("SAMPLE_TABLE", "sample_table")

    import data

    # Mock定義
    mock = mock_dynamodb()
    mock.start()
    table = data.setup_dynamodb_table()
    # DB初期化
    [
        table.put_item(Item=d)
        for d in data.read_json("database/database", float_as=Decimal)
    ]

    request.instance.init = (data, table)

    yield

    mock.stop()


class TestWrapper:
    """DynamoDB Wrapper"""

    def test_case_count1(self):
        """Count1"""

        # Module初期化
        _, table = self.init
        db = DynamoChain(table)
        # 処理実行
        actual = db.count()
        # 結果確認
        expected = 4
        assert expected == actual

    def test_case_count2(self):
        """Count2"""

        # Module初期化
        _, table = self.init
        db = DynamoChain(table)
        # 処理実行
        actual = (
            db.filter(Attr("LastPostedBy").eq("User A"))
            .and_.filter(Attr("Views").eq(1))
            .count_all()
        )
        # 結果確認
        expected = 1
        assert expected == actual

    def test_case_scan1(self):
        """Scan1"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        # 処理実行
        actual = db.scan()
        # 結果確認
        expected = data.read_json("data/expected_scan1", float_as=Decimal)
        assert expected == actual

    def test_case_scan2(self):
        """Scan2"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        # 処理実行
        actual = (
            db.filter(Attr("LastPostedBy").eq("User A"))
            .and_.filter(Attr("Views").eq(1))
            .projection("ForumName, Subject, Message")
            .scan()
        )
        # 結果確認
        expected = data.read_json("data/expected_scan2", float_as=Decimal)
        assert expected == actual

    def test_case_query1(self):
        """Query1"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        # 処理実行
        actual = (
            db.key_condition(Key("ForumName").eq("Amazon DynamoDB"))
            .key_condition(Key("Subject").gte("DynamoDB Thread 1"))
            .filter(Attr("LastPostedBy").eq("User A"))
            .or_.filter(Attr("Views").eq(0))
            .limit(2)
            .desc()
            .consistent_read()
            .query_all()
        )
        # 結果確認
        expected = data.read_json("data/expected_query1", float_as=Decimal)
        assert expected == actual

    def test_case_query2(self):
        """Query2"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        actual = (
            db.key_condition(Key("ForumName").eq("Amazon S3"))
            .key_condition(Key("Subject").gte("S3 Thread 2"))
            .filter(Attr("LastPostedBy").eq("User A"))
            .and_.filter(Attr("Views").eq(1))
            .projection("ForumName, Subject, Message")
            .projection("LastPostedBy, LastPostedDateTime,Views")
            .desc()
            .query_all()
        )
        # 結果確認
        expected = data.read_json("data/expected_query2", float_as=Decimal)
        assert expected == actual

    def test_case_delete1(self):
        """Delete1"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        # 処理実行
        db.key("ForumName", "Amazon S3").key("Subject", "S3 Thread 1").delete()
        # 結果確認
        db.clear()
        actual = db.scan()
        expected = data.read_json("data/expected_delete1", float_as=Decimal)
        assert expected == actual

    def test_case5(self):
        """Get"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        # 処理実行
        actual = (
            db.key("ForumName", "Amazon DynamoDB")
            .key("Subject", "DynamoDB Thread 1")
            .get()
        )
        # 結果確認
        expected = data.read_json("data/expected_get", float_as=Decimal)
        assert expected == actual

    def test_case_put1(self):
        """Put1"""
        """Create a new item"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        # 処理実行

        item = {
            "ForumName": "Amazon S3",
            "Subject": "S3 Thread 3",
            "Message": "S3 thread 3 message",
            "LastPostedBy": "User A",
            "LastPostedDateTime": "2015-09-29T19:58:22.514Z",
            "Views": 2,
            "Replies": 0,
            "Answered": 0,
            "Tags": [
            "largeobjects",
            "multipart upload"
            ]
        }
        db.put(item)
        # 結果確認
        db.clear()
        actual = db.scan()
        expected = data.read_json("data/expected_put1", float_as=Decimal)
        assert expected == actual

    def test_case_put2(self):
        """Put2"""
        """Update existing items"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        # 処理実行
        item = {
            "ForumName": "Amazon S3",
            "Subject": "S3 Thread 2",
            "Message": "S3 thread 2 message",
            "LastPostedBy": "User A",
            "LastPostedDateTime": "2015-09-29T19:58:22.514Z",
            "Views": 2,
            "Replies": 0,
            "Answered": 0,
            "Tags": [
            "largeobjects",
            "multipart upload"
            ]
        }
        db.condition(Key("ForumName").eq("Amazon S3"))\
        .condition(Key("Subject").eq("S3 Thread 2"))\
        .put(item)
        # 結果確認
        db.clear()
        actual = db.scan()
        expected = data.read_json("data/expected_put2", float_as=Decimal)
        assert expected == actual

    def test_case_put3(self):
        """Put3"""
        """Non-existent item is not updated."""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        # 処理実行
        item = {
            "ForumName": "Amazon S3",
            "Subject": "S3 Thread 3",
            "Message": "S3 thread 3 message",
            "LastPostedBy": "User A",
            "LastPostedDateTime": "2015-09-29T19:58:22.514Z",
            "Views": 2,
            "Replies": 0,
            "Answered": 0,
            "Tags": [
            "largeobjects",
            "multipart upload"
            ]
        }
        with raises(ClientError) as e:
            db.condition(Key("ForumName").eq("Amazon S3"))\
            .condition(Key("Subject").eq("S3 Thread 3"))\
            .put(item)
        # 結果確認
        actual =  e.typename
        expected = "ConditionalCheckFailedException"
        assert actual == expected

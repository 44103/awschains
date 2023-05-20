from decimal import Decimal
from typing import Type
from moto import mock_dynamodb
from boto3.dynamodb.conditions import Key, Attr
from pytest import FixtureRequest, MonkeyPatch, fixture
import sys

sys.path.append("../")

from awschains import DynamoChain


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

    def test_case0(self):
        """Count"""

        # Module初期化
        _, table = self.init
        db = DynamoChain(table)
        # 処理実行
        actual = db.count()
        # 結果確認
        expected = 4
        assert expected == actual

    def test_case1(self):
        """Scan"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        # 処理実行
        actual = db.scan()
        # 結果確認
        expected = data.read_json("data/expected_scan", float_as=Decimal)
        assert expected == actual

    def test_case2(self):
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

    def test_case3(self):
        """Query2"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        actual = (
            db.key_condition(Key("ForumName").eq("Amazon S3"))
            .key_condition(Key("Subject").gte("S3 Thread 2"))
            .filter(Attr("LastPostedBy").eq("User A"))
            .and_.filter(Attr("Views").eq(1))
            .desc()
            .query_all()
        )
        # 結果確認
        expected = data.read_json("data/expected_query2", float_as=Decimal)
        assert expected == actual

    def test_case4(self):
        """Delete"""

        # Module初期化
        data, table = self.init
        db = DynamoChain(table)
        # 処理実行
        db.key("ForumName", "Amazon S3").key("Subject", "S3 Thread 1").delete()
        # 結果確認
        db.clear()
        actual = db.scan()
        expected = data.read_json("data/expected_delete", float_as=Decimal)
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

from decimal import Decimal
from moto import mock_dynamodb
from boto3.dynamodb.conditions import Key, Attr
from pytest import fixture
import sys

sys.path.append("../")

from wrapper import MyDynamoDB


@fixture(autouse=True)
def init_modules(request, monkeypatch):
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
    [table.put_item(Item=d) for d in data.read_json("database/database")]

    request.instance.init = [data, table]

    yield

    mock.stop()


class TestWrapper:
    """DynamoDB Wrapper"""

    def test_case0(self):
        """Count"""

        # Module初期化
        _, table = self.init
        db = MyDynamoDB(table)
        # 処理実行
        actual = db.count()
        # 結果確認
        expected = 3
        assert expected == actual

    def test_case1(self):
        """Scan"""

        # Module初期化
        data, table = self.init
        db = MyDynamoDB(table)
        # 処理実行
        actual = db.scan()
        # 結果確認
        expected = data.read_json("data/expected_scan")
        assert expected == actual

    def test_case2(self):
        """Query"""

        # Module初期化
        data, table = self.init
        db = MyDynamoDB(table)
        # 処理実行
        actual = (
            db.key_condition(Key("ForumName").eq("Amazon DynamoDB"))
            .key_condition(Key("Subject").gte("DynamoDB Thread 1"))
            .filter(Attr("LastPostedBy").eq("User A"))
            .ior.filter(Attr("Views").eq(0))
            .limit(2)
            .desc()
            .query_all()
        )
        # 結果確認
        expected = data.read_json("data/expected_query", float_as=Decimal)
        assert expected == actual
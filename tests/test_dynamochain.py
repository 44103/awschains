from decimal import Decimal
from typing import Type
from moto import mock_dynamodb
from boto3.dynamodb.conditions import Key, Attr
from pytest import FixtureRequest, MonkeyPatch, fixture, mark
from src.awschains.dynamochain import PutItem, Query, Scan


@fixture(autouse=True)
def init_modules(request: Type[FixtureRequest], monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "_")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "_")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-1")
    monkeypatch.setenv("SAMPLE_TABLE", "sample_table")

    import data

    # Define mocks
    mock = mock_dynamodb()
    mock.start()
    table = data.setup_dynamodb_table()
    # Init DB
    [
        table.put_item(Item=d)
        for d in data.read_json("database/database", float_as=Decimal)
    ]
    request.instance.init = (data, table)
    yield
    mock.stop()


class TestWrapper:
    """DynamoDB Wrapper"""

    class TestCount:
        """Count"""

        def test_case_1(self):
            """With Scan"""

            # Init modules
            _, table = self.init
            # Execute
            actual = Scan(table).count()
            # Confirm
            expected = 4
            assert expected == actual

        def test_case_2(self):
            """With Query"""

            # Init modules
            _, table = self.init
            # Execute
            actual = (
                Query(table)
                .partition_key_exp(Key("ForumName").eq("Amazon S3"))
                .sort_key_exp(Key("Subject").gte("S3 Thread 2"))
                .filter_exp(Attr("LastPostedBy").eq("User A"))
                .filter_exp(Attr("Views").eq(1))
                .count()
            )
            # Confirm
            expected = 1
            assert expected == actual

    class TestScan:
        """Scan"""

        def test_case_1(self):
            """All items"""

            # Init modules
            data, table = self.init
            # Execute
            actual = Scan(table).run()
            # Confirm
            expected = data.read_json("data/expected_scan1", float_as=Decimal)
            assert expected == actual

        def test_case_2(self):
            """Use filter and projection"""

            # Init modules
            data, table = self.init
            # Execute
            actual = (
                Scan(table)
                .filter_exp(Attr("LastPostedBy").eq("User A"))
                .filter_exp(Attr("Views").eq(1))
                .projection_exp("ForumName, Subject, Message")
                .run()
            )
            # Confirm
            expected = data.read_json("data/expected_scan2", float_as=Decimal)
            assert expected == actual

    class TestQuery:
        """Query"""

        def test_case_1(self):
            """'OR' filter expression"""

            # Init modules
            data, table = self.init
            # Execute
            actual = (
                Query(table)
                .partition_key_exp(Key("ForumName").eq("Amazon DynamoDB"))
                .sort_key_exp(Key("Subject").gte("DynamoDB Thread 1"))
                .filter_exp(Attr("LastPostedBy").eq("User A") | Attr("Views").eq(0))
                .limit(2)
                .desc()
                .consistent_read()
                .run()
            )
            # Confirm
            expected = data.read_json("data/expected_query1", float_as=Decimal)
            assert expected == actual

        def test_case_2(self):
            """'AND' filter expression"""

            # Init modules
            data, table = self.init
            # Execute
            actual = (
                Query(table)
                .key_condition_exp(Key("ForumName").eq("Amazon S3"))
                .key_condition_exp(Key("Subject").gte("S3 Thread 2"))
                .filter_exp(Attr("LastPostedBy").eq("User A"))
                .filter_exp(Attr("Views").eq(1))
                .projection_exp("ForumName, Subject, Message")
                .projection_exp("LastPostedBy, LastPostedDateTime, Views")
                .desc()
                .run()
            )
            # Confirm
            expected = data.read_json("data/expected_query2", float_as=Decimal)
            assert expected == actual

    @mark.skip(reason="Recreate")
    def test_case_delete1(self):
        """Delete1"""

        # Init modules
        data, table = self.init
        db = DynamoChain(table)
        # Execute
        db.key("ForumName", "Amazon S3").key("Subject", "S3 Thread 1").delete()
        # Confirm
        db.clear()
        actual = db.scan()
        expected = data.read_json("data/expected_delete1", float_as=Decimal)
        assert expected == actual

    @mark.skip(reason="Recreate")
    def test_case5(self):
        """Get"""

        # Init modules
        data, table = self.init
        db = DynamoChain(table)
        # Execute
        actual = (
            db.key("ForumName", "Amazon DynamoDB")
            .key("Subject", "DynamoDB Thread 1")
            .get()
        )
        # Confirm
        expected = data.read_json("data/expected_get", float_as=Decimal)
        assert expected == actual

    class TestPutItem:
        """PutItem"""

        def test_case_1(self):
            """Add data via key-value"""

            # Init modules
            data, table = self.init
            # Execute
            actual = (
                PutItem(table)
                .partition_key("ForumName", "Amazon S3")
                .sort_key("Subject", "S3 Thread 3")
                .attr("Message", "S3 thread 3 message")
                .attr("LastPostedBy", "User A")
                .attr("LastPostedDateTime", "2015-09-29T19:58:22.514Z")
                .attr("Views", 2)
                .attr("Replies", 0)
                .attr("Answered", 0)
                .attr("Tags", ["largeobjects", "multipart upload"])
                .run()
            )
            # Confirm
            actual = Scan(table).run()
            expected = data.read_json("data/expected_put1-2", float_as=Decimal)
            assert expected == actual

        def test_case_2(self):
            """Add data via dict"""

            # Init modules
            data, table = self.init
            # Execute
            actual = (
                PutItem(table)
                .attr(
                    {
                        "ForumName": "Amazon S3",
                        "Subject": "S3 Thread 3",
                        "Message": "S3 thread 3 message",
                        "LastPostedBy": "User A",
                        "LastPostedDateTime": "2015-09-29T19:58:22.514Z",
                        "Views": 2,
                        "Replies": 0,
                        "Answered": 0,
                        "Tags": ["largeobjects", "multipart upload"],
                    }
                )
                .run()
            )
            # Confirm
            actual = Scan(table).run()
            expected = data.read_json("data/expected_put1-2", float_as=Decimal)
            assert expected == actual

        def test_case_3(self):
            """Overwrite a item"""

            # Init modules
            data, table = self.init
            # Execute
            actual = (
                PutItem(table)
                .partition_key("ForumName", "Amazon S3")
                .sort_key("Subject", "S3 Thread 2")
                .attr(
                    {
                        "Message": "S3 thread 2 message",
                        "LastPostedBy": "User A",
                        "LastPostedDateTime": "2015-09-29T19:58:22.514Z",
                        "Views": 2,
                        "Replies": 0,
                        "Answered": 0,
                        "Tags": ["largeobjects", "multipart upload"],
                    }
                )
                .run()
            )
            # Confirm
            actual = Scan(table).run()
            expected = data.read_json("data/expected_put3", float_as=Decimal)
            assert expected == actual

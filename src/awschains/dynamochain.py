from abc import ABCMeta, abstractmethod
from itertools import chain
from conditions import ChainsConditionBuilder
from boto3.dynamodb.conditions import (
    ComparisonCondition,
    Equals,
)
from functools import singledispatchmethod


class AccessorBase(metaclass=ABCMeta):
    def __init__(self, table) -> None:
        self._table = table
        self._return_consumed_capacity = "NONE"

    def return_consumed_capacity(self, value: str):
        self._return_consumed_capacity = value
        return self

    @abstractmethod
    def run(self):
        pass


class ReadBase(AccessorBase):
    def __init__(self, table) -> None:
        super().__init__(table)
        self._key_condition_exp = None
        self._projection_exps = []
        self._consistent_read = False

    def projection_exp(self, value: str):
        self._projection_exps.extend([x.strip() for x in value.split(",")])
        return self

    def consistent_read(self, value: bool = True):
        self._consistent_read = value
        return self

    @abstractmethod
    def run(self):
        pass


class WriteBase(AccessorBase):
    def __init__(self, table) -> None:
        super().__init__(table)
        self._condition_exp = None

    def condition_exp(self, value: ComparisonCondition):
        if self._condition_exp:
            self._condition_exp &= value
        else:
            self._condition_exp = value
        return self

    @abstractmethod
    def run(self):
        pass


class MultiReadBase(ReadBase):
    def __init__(self, table) -> None:
        super().__init__(table)
        self._filter_exp = ""
        self._limit = None
        self._select = ""
        self._exclusive_start_key = None

    def limit(self, value: int):
        self._limit = value
        return self

    def filter_exp(self, value):
        if self._filter_exp:
            self._filter_exp &= value
        else:
            self._filter_exp = value
        return self

    def select(self, value: str):
        self._select = value
        return self

    def _create_requests(self):
        requests = {}
        if self._key_condition_exp:
            requests["KeyConditionExpression"] = self._key_condition_exp
        if self._projection_exps:
            requests["ProjectionExpression"] = ",".join(self._projection_exps)
        requests["ConsistentRead"] = self._consistent_read
        if self._filter_exp:
            requests["FilterExpression"] = self._filter_exp
        if self._limit:
            requests["Limit"] = self._limit
        if self._select:
            requests["Select"] = self._select
        if self._exclusive_start_key:
            requests["ExclusiveStartKey"] = self._exclusive_start_key
        return requests

    def run(self):
        return list(chain(*[record["Items"] for record in self.iter()]))

    def count(self):
        self.select("COUNT")
        return sum([record["Count"] for record in self.iter()])

    @abstractmethod
    def iter(self):
        pass


class Scan(MultiReadBase):
    def __init__(self, table) -> None:
        super().__init__(table)

    def iter(self):
        requests = self._create_requests()
        response = self._table.scan(**ChainsConditionBuilder(requests).boto3_query)
        if "LastEvaluetedKey" in response:
            self._exclusive_start_key = response["LastEvaluetedKey"]
        yield response


class Query(MultiReadBase):
    def __init__(self, table) -> None:
        super().__init__(table)
        self._scan_index_forward = True

    def key_condition_exp(self, value: ComparisonCondition):
        if self._key_condition_exp:
            self._key_condition_exp &= value
        else:
            self._key_condition_exp = value
        return self

    def partition_key_exp(self, value: Equals):
        return self.key_condition_exp(value)

    def sort_key_exp(self, value: ComparisonCondition):
        return self.key_condition_exp(value)

    def asc(self):
        self._scan_index_forward = True
        return self

    def desc(self):
        self._scan_index_forward = False
        return self

    def _create_requests(self):
        requests = super()._create_requests()
        requests["ScanIndexForward"] = self._scan_index_forward
        return requests

    def iter(self):
        requests = self._create_requests()
        response = self._table.query(**ChainsConditionBuilder(requests).boto3_query)
        if "LastEvaluetedKey" in response:
            self._exclusive_start_key = response["LastEvaluetedKey"]
        yield response


class PutItem(WriteBase):
    def __init__(self, table) -> None:
        super().__init__(table)
        self._item = {}

    def item(self, key, value):
        self._item |= {key: value}
        return self

    @singledispatchmethod
    def partition_key(self, key: str, value: str):
        return self.item(key, value)

    @singledispatchmethod
    def sort_key(self, key: str, value: str):
        return self.item(key, value)

    @partition_key.register
    @sort_key.register
    def _(self, value: Equals):
        expression = value.get_expression()
        key = expression["values"][0].name
        val = expression["values"][1]
        return self.item(key, val)

    @singledispatchmethod
    def attr(self, key: str, value: str):
        return self.item(key, value)

    @attr.register
    def _(self, item: dict):
        self._item |= item
        return self

    def run(self):
        requests = {"Item": self._item}
        if self._condition_exp:
            requests["ConditionExpression"] = self._condition_exp
        self._table.put_item(**requests)

from abc import ABCMeta, abstractmethod
from itertools import chain
from conditions import ChainsConditionBuilder


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

    def key_condition_exp(self, value):
        if self._key_condition_exp:
            self._key_condition_exp &= value
        else:
            self._key_condition_exp = value
        return self

    def projection_exp(self, pe: str):
        self._projection_exps.extend([x.strip() for x in pe.split(",")])
        return self

    def consistent_read(self, cr: bool = True):
        self._consistent_read = cr
        return self

    @abstractmethod
    def run(self):
        pass


class WriteBase(AccessorBase):
    def __init__(self, table) -> None:
        super().__init__(table)
        self._condition_exp = ""

    def condition_exp(self, ce: str):
        self._condition_exp(ce)
        return self

    @abstractmethod
    def run(self):
        pass


class MultiReadBase(ReadBase):
    def __init__(self, table) -> None:
        super().__init__(table)
        self._filter_exp = ""
        self._limit = None
        self._exclusive_start_key = None

    def limit(self, value: int):
        self._limit = value
        return self

    def filter_exp(self, fe):
        if self._filter_exp:
            self._filter_exp &= fe
        else:
            self._filter_exp = fe
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
        if self._exclusive_start_key:
            requests["ExclusiveStartKey"] = self._exclusive_start_key
        return requests

    def run(self):
        return list(chain(*[record["Items"] for record in self.iter()]))

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

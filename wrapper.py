import operator


class MyDynamoDB:
    def __init__(self, table) -> None:
        self._table = table
        self._query = {}
        self._operator = operator.iand

    def _check_next_query(self, resp):
        if "LastEvaluetedKey" in resp:
            self._query["ExclusiveStartKey"] = resp["LastEvaluetedKey"]
        else:
            self._query.pop("ExclusiveStartKey", None)

    def clear(self):
        self._query = {}
        self._operator = operator.iand

    @property
    def iand(self):
        self._operator = operator.iand
        return self

    @property
    def ior(self):
        self._operator = operator.ior
        return self

    @property
    def done(self):
        return "ExclusiveStartKey" in self._query

    # Chain Method
    def key_condition(self, kce):
        if "KeyConditionExpression" in self._query:
            self._query["KeyConditionExpression"] &= kce
        else:
            self._query["KeyConditionExpression"] = kce
        return self

    def filter(self, fe):
        if "FilterExpression" in self._query:
            self._operator(self._query["FilterExpression"], fe)
        else:
            self._query["FilterExpression"] = fe
        return self

    def limit(self, num):
        self._query["Limit"] = num
        return self

    def asc(self):
        self._query["ScanIndexForward"] = True
        return self

    def desc(self):
        self._query["ScanIndexForward"] = False
        return self

    # Last Method
    def count(self):
        self._query["Select"] = "COUNT"
        resp = self._table.scan(**self._query)
        self._check_next_query(resp)
        count = resp["Count"]
        return count

    def count_all(self):
        resp = self.count()
        while self.done:
            resp += self.count()
        return resp

    def scan(self):
        resp = self._table.scan(**self._query)
        self._check_next_query(resp)
        return resp["Items"]

    def scan_all(self):
        resp = self.scan()
        while self.done:
            resp += self.scan()
        return resp

    def query(self):
        resp = self._table.query(**self._query)
        self._check_next_query(resp)
        return resp["Items"]

    def query_all(self):
        resp = self.query()
        while self.done:
            resp += self.query()
        return resp

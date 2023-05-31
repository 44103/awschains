from abc import ABCMeta, abstractmethod
from conditions import ChainsCondition

class AccessorBase(metaclass=ABCMeta):
    def __init__(self, table) -> None:
        self.table = table
        self.condition = ChainsCondition()

    @abstractmethod
    def return_consumed_capacity(self):
        print("return_consumed_capacity")

    @abstractmethod
    def run(self):
        print("run")

class ReadBase(AccessorBase):
    @abstractmethod
    def projection_exp(self, pe: str):
        print("projection_exp")
        self.condition.projection_exp(pe)

    @abstractmethod
    def consistent_read(self, cr: bool = True):
        print("consistent_read")
        self.condition.consistent_read(cr)

class WriteBase(AccessorBase):
    @abstractmethod
    def condition_exp(self, ce: str):
        print("condition_exp")
        self.condition.condition_exp(ce)

class MultiReadBase(ReadBase):
    @abstractmethod
    def asc(self):
        print("asc")
        self.condition.asc()

    @abstractmethod
    def desc(self):
        print("desc")
        self.condition.desc()

    @abstractmethod
    def limit(self, num):
        print("limit")
        self.condition.limit(num)

    @abstractmethod
    def filter_exp(self, fe):
        print("fileter_exp")
        self.condition.filter_exp(fe)

class GetItem(ReadBase):
    def __init__(self, table) -> None:
        super().__init__(table)

    def projection_exp(self, pe: str):
        super().projection_exp(pe)
        return self

    def consistent_read(self, cr: bool = True):
        super().consistent_read(cr)
        return self

    def return_consumed_capacity(self):
        super().return_consumed_capacity()
        return self

    def run(self):
        super().run()
        print(self.condition._projection_exp)

get_item = GetItem("table")
(
    get_item
    .consistent_read()
    .projection_exp("pe")
    .projection_exp("aaa,bbb,ccc")
    .return_consumed_capacity()
    .run()
)
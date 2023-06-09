from boto3.dynamodb.conditions import Attr, ConditionExpressionBuilder


class ChainsCondition:
    def __init__(self):
        self._key = {}
        self._projection_exp = ""
        self._key_condition_exp = None
        self._filter_exp = None
        self._scan_index_forward = None
        self._consistent_read = None

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value: dict):
        self._key |= value

    def key_condition_exp(self, value):
        if not self._key_condition_exp:
            self._key_condition_exp &= value
        else:
            self._key_condition_exp = value

    def projection_exp(self, value):
        if not self._projection_exp:
            self._projection_exp += "," + value
        else:
            self._projection_exp = value

    def asc(self):
        self._scan_index_forward = True

    def desc(self):
        self._scan_index_forward = False

    def limit(self, num: int):
        self._limit = num

    def condition_exp(self, ce):
        self._condition_exp = ce

    def filter_exp(self, fe):
        self._filter_exp = fe

    def consistent_read(self, cr=True):
        self._consistent_read = cr


class ChainsConditionBuilder(ConditionExpressionBuilder):
    def __init__(self, query: ChainsCondition):
        super().__init__()
        self._query = query

    def _build_key_condition_expression(self):
        self._build_expression_to_query("KeyConditionExpression", is_key_condition=True)

    def _build_filter_expression(self):
        self._build_expression_to_query("FilterExpression")

    def _build_expression_to_query(self, key, is_key_condition=False):
        if key not in self._query:
            return
        bce = self.build_expression(self._query[key], is_key_condition=is_key_condition)
        self.expression_attribute_names |= bce.attribute_name_placeholders
        self.expression_attribute_values |= bce.attribute_value_placeholders
        self._query[key] = bce.condition_expression

    def _build_projection_expression(self):
        if "ProjectionExpression" not in self._query:
            return
        if type(self._query["ProjectionExpression"]) is not str:
            raise TypeError("ProjectionExpression should have str specified.")
        self._add_attr_names_from_pe()
        self._build_pe_from_attr_names()

    def _add_attr_names_from_pe(self):
        pe_list = [
            key.strip() for key in self._query["ProjectionExpression"].split(",")
        ]
        for pe in pe_list:
            if pe not in self.expression_attribute_names.values():
                bce = self.build_expression(Attr(pe).eq("_"))
                self.expression_attribute_names |= bce.attribute_name_placeholders

    def _build_pe_from_attr_names(self):
        reversed_attr_names = {v: k for k, v in self.expression_attribute_names.items()}
        pe_list = [
            key.strip() for key in self._query["ProjectionExpression"].split(",")
        ]
        self._query["ProjectionExpression"] = ",".join(
            [reversed_attr_names[pe] for pe in pe_list]
        )

    @property
    def expression_attribute_names(self):
        if "ExpressionAttributeNames" not in self._query:
            return {}
        else:
            return self._query["ExpressionAttributeNames"]

    @expression_attribute_names.setter
    def expression_attribute_names(self, value):
        if "ExpressionAttributeNames" not in self._query:
            self._query["ExpressionAttributeNames"] = value
        else:
            self._query["ExpressionAttributeNames"] |= value

    @property
    def expression_attribute_values(self):
        if "ExpressionAttributeValues" not in self._query:
            return {}
        else:
            return self._query["ExpressionAttributeValues"]

    @expression_attribute_values.setter
    def expression_attribute_values(self, value):
        if "ExpressionAttributeValues" not in self._query:
            self._query["ExpressionAttributeValues"] = value
        else:
            self._query["ExpressionAttributeValues"] |= value

    @property
    def boto3_query(self):
        # ここでboto3用のqueryを生成して
        self._build_key_condition_expression()
        self._build_filter_expression()
        self._build_projection_expression()
        return self._query

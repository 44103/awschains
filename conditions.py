from boto3.dynamodb.conditions import ConditionExpressionBuilder, Attr

class ChainsConditionBuilder(ConditionExpressionBuilder):

    def __init__(self, query):
        super().__init__()
        self._query = query
        self._pe_list = []

    def _build_key_condition_expression(self):
        if "KeyConditionExpression" not in self._query:
            return
        bce = self.build_expression(self._query["KeyConditionExpression"])
        self._save_expression_to_query(
            self.build_expression(self._query["KeyConditionExpression"]),
            "KeyConditionExpression"
        )

    def _build_filter_expression(self):
        if "FilterExpression" not in self._query:
            return
        self._save_expression_to_query(
            self.build_expression(self._query["FilterExpression"], is_key_condition= False),
            "FilterExpression"
        )

    def _save_expression_to_query(self, bce, key):
        self.expression_attribute_names |= bce.attribute_name_placeholders
        self.expression_attribute_values |= bce.attribute_value_placeholders
        self._query[key] = bce.condition_expression

    def _build_projection_expression(self):
        if "ProjectionExpression" not in self._query:
            return
        if type(self._query["ProjectionExpression"]) is not str:
            raise TypeError("ProjectionExpression should have str specified.")
        self._add_attr_names_from_pe()
        self._use_attr_names_for_pe()

    def _add_attr_names_from_pe(self):
        self._query["ProjectionExpression"].replace(" ", "")
        self._pe_list = self._query["ProjectionExpression"].split(",")
        for pe in self._pe_list:
            if pe not in self.expression_attribute_names.values():
                bce = self.build_expression(Attr(pe).eq("dummy"))
                self.expression_attribute_names |= bce.attribute_name_placeholders

    def _use_attr_names_for_pe(self):
        reversed_attr_names = {v: k for k, v in self.expression_attribute_names.items()}
        for i, pe in enumerate(self._pe_list):
            self._pe_list[i] = reversed_attr_names[pe]
        self._query["ProjectionExpression"] = ",".join(self._pe_list)

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
    def query(self):
        self._build_key_condition_expression()
        self._build_filter_expression()
        self._build_projection_expression()
        return self._query

    @query.setter
    def query(self, value):
        self._query = value
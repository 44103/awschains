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
        self._query["ExpressionAttributeNames"] |= bce.attribute_name_placeholders
        self._query["ExpressionAttributeValues"] |= bce.attribute_value_placeholders
        self._query["KeyConditionExpression"] = bce.condition_expression

    def _build_filter_expression(self):
        if "FilterExpression" not in self._query:
            return
        bce = self.build_expression(self._query["FilterExpression"], is_key_condition= False)
        self._query["ExpressionAttributeNames"] |= bce.attribute_name_placeholders
        self._query["ExpressionAttributeValues"] |= bce.attribute_value_placeholders
        self._query["FilterExpression"] = bce.condition_expression

    def _build_projection_expression(self):
        if "ProjectionExpression" not in self._query:
            return
        if type(self._query["ProjectionExpression"]) != str:
            raise TypeError("ProjectionExpression should have str specified.")
        self._add_pelist_to_atts()
        self._convert_pelist_to_pe()

    def _add_pelist_to_atts(self):
        self._query["ProjectionExpression"].replace(" ", "")
        self._pe_list = self._query["ProjectionExpression"].split(",")
        for pe in self._pe_list:
            if pe not in self._query["ExpressionAttributeNames"].values():
                _, atts, _ = self.build_expression(Attr(pe).eq("dummy"))
                self._query["ExpressionAttributeNames"] |= atts

    def _convert_pelist_to_pe(self):
        inverted_atts = {v: k for k, v in self._query["ExpressionAttributeNames"].items()}
        for i, pe in enumerate(self._pe_list):
            self._pe_list[i] = inverted_atts[pe]
        self._query["ProjectionExpression"] = ",".join(self._pe_list)

    @property
    def query(self):
        self._query["ExpressionAttributeNames"] = {}
        self._query["ExpressionAttributeValues"] = {}
        self._build_key_condition_expression()
        self._build_filter_expression()
        self._build_projection_expression()
        return self._query

    @query.setter
    def query(self, value):
        self._query = value
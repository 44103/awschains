
[EN](./README.md) | JA

[![Python](https://img.shields.io/badge/Python-3.9-blue?logo=python)](https://hub.docker.com/layers/library/python/3.9/images/sha256-c65dadac8789fed40962578392e99a0528dcb868442c75d144e68ba858984837?context=explore)
[![License](https://img.shields.io/badge/Apache-2.0-D22128?logo=apache)](LICENSE)
[![CI](https://github.com/44103/awschain/actions/workflows/main.yml/badge.svg)](https://github.com/44103/awschain/actions/workflows/main.yml)

-----

# AWS Chains
AWS Chainsはboto3をメソッドチェーンで記述するためのラッパーです。

## DynamoChain
### 使い方
1. インスタンス作成
   ```python
   db = DynamoChain(table)
   ```
1. DynamoDBにクエリ
   ```python
   result = (
      db.key_condition(Key("ForumName").eq("Amazon DynamoDB"))
      .key_condition(Key("Subject").gte("DynamoDB Thread 1"))
      .filter(Attr("LastPostedBy").eq("User A"))
      .ior.filter(Attr("Views").eq(0))
      .limit(2)
      .desc()
      .query_all()
   )
   ```

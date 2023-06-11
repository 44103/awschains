EN | [JA](/README.ja.md)

[![License](https://img.shields.io/badge/Apache-2.0-D22128?logo=apache)](LICENSE)
[![Language](https://img.shields.io/badge/Python-3.9-3776AB?logo=python)](https://hub.docker.com/layers/library/python/3.9/images/sha256-c65dadac8789fed40962578392e99a0528dcb868442c75d144e68ba858984837?context=explore)
[![Test](https://github.com/44103/awschains/actions/workflows/test.yml/badge.svg)](https://github.com/44103/awschains/actions/workflows/test.yml)

---

# AWS Chains

AWS Chains is a wrapper for writing boto3 as method chain.

## DynamoChain

### How to Use

1. Query to DynamoDB
   ```python
   result = (
       Query(table)
       .partition_key_exp(Key("ForumName").eq("Amazon S3"))
       .sort_key_exp(Key("Subject").gte("S3 Thread 2"))
       .filter_exp(Attr("LastPostedBy").eq("User A"))
       .filter_exp(Attr("Views").eq(1))
       .projection_exp("ForumName, Subject, Message")
       .projection_exp("LastPostedBy, LastPostedDateTime, Views")
       .desc()
       .run()
   )
   ```

### Signature Feature

#### Code Completion

![img](./img/code-completion.gif)

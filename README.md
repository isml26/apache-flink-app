
# README

## Introduction


## Apache Kafka and Apache Flink

If you are using Apache Kafka today, Apache Flink should be a crucial piece of your technology stack to ensure you're extracting what you need from your real-time data. With the combination of Apache Flink and Apache Kafka, the open-source event streaming possibilities become exponential.

For more information, you can refer to the blog post by IBM titled [Apache Kafka and Apache Flink: An Open Source Match Made in Heaven](https://www.ibm.com/blog/apache-kafka-and-apache-flink-an-open-source-match-made-in-heaven/).

- [Flink Architecture](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/flink-architecture/)
- [Kafka Architecture](https://kafka.apache.org/10/documentation/streams/architecture)

![Blank diagram](https://github.com/isml26/apache-flink-app/assets/62605922/d29c470f-b2c2-4cdf-8832-4b0d99adde8b)


### Commands in docker
```
docker exec -it broker /bin/bash
```
```
kafka-console-consumer --topic financial_transactions --bootstrap-server broker:29092 
```
```
kafka-console-consumer --topic financial_transactions --bootstrap-server broker:29092 --from-beginning
```
```
 docker network ls
```
```
docker network inspect <network_name>
```
```
- apt-get update -y
- apt-get install -y iputils-ping
- apt-get install -y telnet
- sudo docker exec -it [container] cat /etc/hosts
- psql -U postgres
- \l ( list all databases)  or  \d (describe database objects such as tables, views, sequences, and indexes)
```


# Elasticsearch Queries

### Get Transactions

```
GET transactions
```

```
GET transactions/_search
```

### Reindex

```
POST _reindex
{
  "source": {
    "index": "transactions"
  },
  "dest": {
    "index": "transaction_part1"
  },
  "script": {
    "source": """
      ctx._source.transactionDate = new Date(ctx._source.transactionDate).toString();
    """
  }
}
```

```
POST _reindex
{
  "source": {
    "index": "transactions"
  },
  "dest": {
    "index": "transaction_part2"
  },
  "script": {
    "source": """
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      formatter.setTimeZone(TimeZone.getTimeZone('UTC'));
      ctx._source.transactionDate = formatter.format(new Date(ctx._source.transactionDate));
    """
  }
}
```

### Kibana Visualization
![kb2](https://github.com/isml26/apache-flink-app/assets/62605922/5607e5a7-36c4-479f-9d5d-08949ec36c84)

### Flink
![sink](https://github.com/isml26/apache-flink-app/assets/62605922/50f7906b-68e4-44a6-a8eb-e26df131734c)
![sink2](https://github.com/isml26/apache-flink-app/assets/62605922/5b094a62-f201-4f71-9137-df63358fbcf3)
![sink3](https://github.com/isml26/apache-flink-app/assets/62605922/96c1617b-f3ad-4297-a669-c3264a575bd1)

### Aggregations
![aggreagte](https://github.com/isml26/apache-flink-app/assets/62605922/0b15b384-e01c-41f0-a250-321a3dc5a7f6)


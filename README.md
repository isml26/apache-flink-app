
# README

## Introduction


## Apache Kafka and Apache Flink

If you are using Apache Kafka today, Apache Flink should be a crucial piece of your technology stack to ensure you're extracting what you need from your real-time data. With the combination of Apache Flink and Apache Kafka, the open-source event streaming possibilities become exponential.

For more information, you can refer to the blog post by IBM titled [Apache Kafka and Apache Flink: An Open Source Match Made in Heaven](https://www.ibm.com/blog/apache-kafka-and-apache-flink-an-open-source-match-made-in-heaven/).

- [Flink Architecture](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/flink-architecture/)
- [Kafka Architecture](https://kafka.apache.org/10/documentation/streams/architecture)

### Commands in docker
- docker exec -it broker /bin/bash
- kafka-console-consumer --topic financial_transactions --bootstrap-server broker:29092 
- kafka-console-consumer --topic financial_transactions --bootstrap-server broker:29092 --from-beginning
- docker network ls
- docker network inspect <network_name>
- apt-get update -y
- apt-get install -y iputils-ping
- apt-get install -y telnet
- sudo docker exec -it [container] cat /etc/hosts
- psql -U postgres
- \l ( list all databases)  or  \d (describe database objects such as tables, views, sequences, and indexes)
- 
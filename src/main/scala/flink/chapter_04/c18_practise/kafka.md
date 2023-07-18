# Kafka operation

1. go to kafka

```shell
/opt/modules/kafka/kafka_2.13-3.3.1
```

2. start zk

```shell
 bin/zookeeper-server-start.sh config/zookeeper.properties
```

3. start kafka

```shell
bin/kafka-server-start.sh config/server.properties
```

4. create topic

```shell
 bin/kafka-topics.sh --create --topic topictest --bootstrap-server localhost:9092
```

5. start producer

```shell
bin/kafka-console-producer.sh --topic topictest --bootstrap-server localhost:9092
```

6. start consumer

```shell
 bin/kafka-console-consumer.sh --topic topictest --from-beginning --bootstrap-server localhost:9092
```
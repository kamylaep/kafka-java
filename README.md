# Kafka

Small (and very simple) app that implements and tests pure Java Kafka Producers and Consumers.

You can use this [docker compose file](docker/docker-compose.yml) to create a single node Kafka cluster.

The topic used can be created using 

```bash
$ docker exec kafka kafka-topics --zookeeper zookeeper:2181 --topic fibonacci --create --partitions 1 --replication-factor 1
```
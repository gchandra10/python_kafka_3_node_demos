# Docker Kafka - Kafka UI

## Fork and Clone the Repo

```
git clone https://github.com/gchandra10/docker_kafka_ui
```

## Launch the containers

```
docker compose up -d
```

## Open the UI in browser

http://localhost:8080


## Login to container

```
docker exec --workdir /opt/kafka/bin/ -it kafka sh
```

## Create a Topic

```
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic gc-topic
 ```


## Start the Producer

```
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic gc-topic
```


## Start the consumer

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic gc-topic --from-beginning
```


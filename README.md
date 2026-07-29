# 3 Node Kafka & Kafka UI using Docker

```
docker compose up -d
```

## Open the UI in browser

http://localhost:8080

create a topic gctopic & gctopic_m with 3 partitions. Leave other settings as is.

----

**Basic Demos (uses Kafka-Python Library)**

These demos uses the following library `git+https://github.com/dpkp/kafka-python.git`

**Sync the Folder so necessary libraries are downloaded.**

```
uv sync
```

```
cd basic-demos
```

**Publish first message**

```
uv run python 01_producer_simple.py "Hello World"
```

**Simple Consumer with default Group**

*Press Ctrl+C to end the consumer*

```
uv run python 02_consumer_simple_autocommit.py
```

**Consumers with specific Group IDs**

```
uv run python 02_consumer_simple_autocommit.py --group gcgroupA
```

```
uv run python 02_consumer_simple_autocommit.py --group gcgroupA --topics gctopic
```

## Second Example reading from API and publishing to Kafka

```
uv run python joke_producer.py --topic gctopic_m --interval 2
```

**Simple Consumer with default Group**

*Press Ctrl+C to end the consumer*

```
uv run python 02_consumer_simple_autocommit.py
```

**Consumers with specific Group IDs**

```
uv run python 02_consumer_simple_autocommit.py --group gcgroupA
```

```
uv run python 02_consumer_simple_autocommit.py --group gcgroupA --topics gctopic
```

## Publish to specific or random partitions

```
uv run python 01b_producer_multipartition.py --mode random
```

```
uv run python 01b_producer_multipartition.py --mode same-key
```

```
uv run python 01b_producer_multipartition.py --mode explicit --partition 0
```

**Simple Consumer with default Group**

```
uv run python 02_consumer_simple_autocommit.py
```

```
uv run python 01c_producer_batch.py
```

```
uv run python 02c_consumer_batch.py --group batch1
```

```
uv run python 02c_consumer_batch.py --group batch2 --max-records 1000
```

**Intermediate Demos (uses Confluent Kafka)**

```
uv add confluent-kafka
```

```
uv run streamlit run st_unified_webapp.py --server.address 0.0.0.0 --server.port 8501
```

```
uv run python st_unified_consumer.py
```
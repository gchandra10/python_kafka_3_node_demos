# 3 Node Kafka & Kafka UI using Docker

```
docker compose up -d
```

## Open the UI in browser

http://localhost:8080

create a topic **gctopic** & **gctopic_m** with 3 partitions 

**Basic Demos (uses Kafka-Python Library)**

```
uv add git+https://github.com/dpkp/kafka-python.git
```

```
cd basic-demos
```


```
uv run python 01_producer_simple.py message

uv run python 02_consumer_simple_autocommit.py
uv run python 02_consumer_simple_autocommit.py --group gcgroupA
uv run python 02_consumer_simple_autocommit.py --group gcgroupA --topics gctopic
```

```
uv run python joke_producer.py --topic gctopic_m --interval 2

uv run python 02_consumer_simple_autocommit.py
uv run python 02_consumer_simple_autocommit.py --group gcgroupA
uv run python 02_consumer_simple_autocommit.py --group gcgroupA --topics gctopic
```

```
uv run python 01b_producer_multipartition.py --mode random
uv run python 01b_producer_multipartition.py --mode same-key
uv run python 01b_producer_multipartition.py --mode explicit --partition 0

uv run python 02_consumer_simple_autocommit.py
```

```
uv run python 01c_producer_batch.py
uv run python 02c_consumer_batch.py --group batch1
uv run python 02c_consumer_batch.py --group batch2 --max-records 1000

```

**Intermediate Demos (uses Confluent Kafka)**

```
uv add confluent-kafka
```

```
uv run streamlit run st_unified_webapp.py --server.address 0.0.0.0 --server.port 8501

uv run python st_unified_consumer.py
```
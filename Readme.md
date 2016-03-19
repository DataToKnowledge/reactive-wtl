# Reactive WTL

extraction of feed, and articles from news papers and feed using akka stream

- extract feeds
- parsing and main article extraction
- wikipedia entities extraction
- dbpedia annotations extraction
- focus location extraction


## Kafka Compacted topic creation

log into kafka server and runt the following commmands

```bash

kafka-topics.sh --zookeeper zoo-1:2181,zoo-2:2181 --create --topic feeds --replication-factor 2 \
    --partition 3 --config cleanup.policy=compact --config retention.ms=86400000
    
kafka-topics.sh --zookeeper zoo-1:2181,zoo-2:2181 --create --topic query_terms --replication-factor 2 \
    --partition 3 --config cleanup.policy=compact --config retention.ms=86400000
    
kafka-topics.sh --zookeeper zoo-1:2181,zoo-2:2181 --create --topic feed_items --replication-factor 2 \
    --partition 3 --config cleanup.policy=compact
    
kafka-topics.sh --zookeeper zoo-1:2181,zoo-2:2181 --create --topic articles --replication-factor 2 \
    --partition 3 --config cleanup.policy=compact

```


Test
```bash

kafka-topics.sh --zookeeper 192.168.99.100:2181 --create --topic feeds --replication-factor 1 \
    --partition 3 --config cleanup.policy=compact --config retention.ms=86400000
    
kafka-topics.sh --zookeeper 192.168.99.100:2181 --create --topic query_terms --replication-factor 1 \
    --partition 3 --config cleanup.policy=compact --config retention.ms=86400000
    
kafka-topics.sh --zookeeper 192.168.99.100:2181 --create --topic feed_items --replication-factor 1 \
    --partition 3 --config cleanup.policy=compact
    
kafka-topics.sh --zookeeper 192.168.99.100:2181 --create --topic articles --replication-factor 1 \
    --partition 3 --config cleanup.policy=compact
```
# Reactive WTL

extraction of feed, and articles from news papers and feed using akka stream

- extract feeds
- parsing and main article extraction
- wikipedia entities extraction
- dbpedia annotations extraction
- focus location extraction

## Setup

### Kafka Compacted topic creation

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

### Index Initialization
To init the index 



2. log to jupyter notebook and add query terms

### Start services


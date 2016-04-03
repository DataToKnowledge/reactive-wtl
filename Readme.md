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

### Check all services are running

- influxdb
- kafka
- dbpedia
- elastic
- jupyter

### Index Initialization
To init the index 

1. run the docker `docker run -it --rm data2knowledge/reactive-wtl:1.2.0 -e docker -j InitIndex`
2. log to jupyter notebook and run the notebook `query_term_indexer_test.ipynb`

### Start services

SERVICES: TermsToKafka, FeedsToKafka, ProcessTerms, ProcessFeeds, TagArticles, ToElastic, InitIndex

1. run 1 instance of the docker `docker run -dt --name terms2kafka data2knowledge/reactive-wtl:1.2.0 -e docker -j TermsToKafka`
2. run the docker `docker run -dt --name ProcessTerms data2knowledge/reactive-wtl:1.1.1 -e docker -j ProcessTerms` ran 3 instances
3. run 1 instance of the docker `docker run -dt --name FeedsToKafka data2knowledge/reactive-wtl:1.2.0 -e docker -j FeedsToKafka`
4. run the docker `docker run -dt --name ProcessFeeds data2knowledge/reactive-wtl:1.2.0 -e docker -j ProcessFeeds` ran 3 instances
4. run the docker `docker run -dt --name TagArticles data2knowledge/reactive-wtl:1.2.0 -e docker -j TagArticles` ran 3 instances
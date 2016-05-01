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

kafka-topics.sh --zookeeper zoo-1:2181,zoo-2:2181 --create --topic feed_items --replication-factor 2 \
    --partition 3 --config cleanup.policy=compact

kafka-topics.sh --zookeeper zoo-1:2181,zoo-2:2181 --create --topic articles --replication-factor 2 \
    --partition 3 --config cleanup.policy=compact

```

kafka-topics.sh --zookeeper 192.168.99.100:2181 --create --topic feed_items --replication-factor 1 \
    --partition 3 --config cleanup.policy=compact

### Check all services are running

- influxdb
- kafka
- dbpedia
- elastic
- jupyter

### Index Initialization
To init the index

1. run the docker `docker run -it --rm data2knowledge/reactive-wtl:1.6.2 -e docker -j InitIndex`
2. log to jupyter notebook and run the notebook `query_term_indexer_test.ipynb`

### Start services

SERVICES: TermsToKafka, FeedsToKafka, ProcessTerms, ProcessFeeds, TagArticles, ToElastic, InitIndex

1. run the docker `docker rm -f GoogleNews && docker run -dt --name GoogleNews data2knowledge/reactive-wtl:1.6.2 -e docker -j GoogleNews` ran 2 instances
2. run the docker `docker rm -f ProcessFeeds && docker run -dt --name ProcessFeeds data2knowledge/reactive-wtl:1.6.2 -e docker -j ProcessFeeds` ran 3 instances
3. run the docker `docker rm -f TagArticles && docker run -dt --name TagArticles data2knowledge/reactive-wtl:1.6.2 -e docker -j TagArticles` ran 4 instances
4. run the docker `docker rm -f ToElastic && docker run -dt --name ToElastic data2knowledge/reactive-wtl:1.6.2 -e docker -j ToElastic` ran 2 instances

Not
5. run the docker `docker rm -f FeedFromItems && docker run -dt --name FeedFromItems data2knowledge/reactive-wtl:1.6.2 -e docker -j FeedsFromItems` ran 1 instance

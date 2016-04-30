package it.dtk.reactive.util

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Producer.Result
import akka.kafka.scaladsl._
import akka.kafka.{ ConsumerSettings, ProducerSettings }
import akka.stream.scaladsl.Flow
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._

/**
 * Created by fabiofumarola on 23/04/16.
 */
object KafkaUtils {

  def consumerSettings[K, V](brokers: String, groupId: String, clientId: String, topic: String,
    kDes: Deserializer[K], vDes: Deserializer[V])(implicit system: ActorSystem) = {

    ConsumerSettings(system, kDes, vDes, Set(topic))
      .withBootstrapServers(brokers)
      .withGroupId(groupId)
      .withClientId(clientId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  }

  def produceSettings[K, V](brokers: String, topic: String,
    kSer: Serializer[K], vSer: Serializer[V])(implicit system: ActorSystem) = {
    ProducerSettings(system, kSer, vSer)
      .withBootstrapServers(brokers)
  }

  def simpleSource[K, V](brokers: String, groupId: String, topic: String,
    kDes: Deserializer[K], vDes: Deserializer[V])(implicit system: ActorSystem) = {
    Consumer.plainSource(consumerSettings(brokers, groupId, groupId, topic, kDes, vDes))
  }

  def atMostOnceSource[K, V](brokers: String, groupId: String, clientId: String, topic: String,
    kDes: Deserializer[K], vDes: Deserializer[V])(implicit system: ActorSystem) = {
    Consumer.atMostOnceSource(consumerSettings(brokers, groupId, clientId, topic, kDes, vDes))
  }

  def flowProducer[K, V](brokers: String, topic: String, kSer: Serializer[K], vSer: Serializer[V])(implicit system: ActorSystem) = {
    Producer.flow(produceSettings(brokers, topic, kSer, vSer))
  }

  def plainProducer[K, V](brokers: String, topic: String, kSer: Serializer[K], vSer: Serializer[V])(implicit system: ActorSystem) = {
    Producer.plainSink(produceSettings(brokers, topic, kSer, vSer))
  }
}

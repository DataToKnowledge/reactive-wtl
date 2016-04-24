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

  implicit val system: ActorSystem = implicitly[ActorSystem]

  def consumerSettings[K, V](brokers: String, groupId: String, topic: String,
    kDes: Deserializer[K], vDes: Deserializer[V]) = {

    ConsumerSettings(system, kDes, vDes, Set(topic))
      .withBootstrapServers(brokers)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  }

  def produceSettings[K, V](brokers: String, topic: String,
    kSer: Serializer[K], vSer: Serializer[V]) = {
    ProducerSettings(system, kSer, vSer)
      .withBootstrapServers(brokers)
  }

  def simpleSource[K, V](brokers: String, groupId: String, topic: String,
    kDes: Deserializer[K], vDes: Deserializer[V]) = {
    Consumer.plainSource(consumerSettings(brokers, groupId, topic, kDes, vDes))
  }

  def atMostOnceSource[K, V](brokers: String, groupId: String, topic: String,
    kDes: Deserializer[K], vDes: Deserializer[V]) = {
    Consumer.atMostOnceSource(consumerSettings(brokers, groupId, topic, kDes, vDes))
  }

  def flowProducer[K, V](brokers: String, topic: String, kSer: Serializer[K], vSer: Serializer[V]): Flow[Producer.Message[K, V, Nothing], Result[K, V, Nothing], NotUsed] = {
    Producer.flow(produceSettings(brokers, topic, kSer, vSer))
  }

  def plainProducer[K, V](brokers: String, topic: String, kSer: Serializer[K], vSer: Serializer[V]) = {
    Producer.plainSink(produceSettings(brokers, topic, kSer, vSer))
  }
}

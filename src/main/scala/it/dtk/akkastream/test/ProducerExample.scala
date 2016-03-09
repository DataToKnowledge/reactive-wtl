package it.dtk.akkastream.test

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import com.softwaremill.react.kafka.KafkaMessages._
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerMessage, ProducerProperties, ReactiveKafka}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringDeserializer, StringSerializer}
import org.reactivestreams.{Publisher, Subscriber}

/**
  * Created by fabiofumarola on 07/03/16.
  */
object ProducerExample extends App {

  implicit val actorSystem = ActorSystem("ReactiveKafka")
  implicit val materializer = ActorMaterializer()

  implicit val executor = actorSystem.dispatcher

  val kafka = new ReactiveKafka()

  val publisher: Publisher[StringConsumerRecord] = kafka.consume(ConsumerProperties(
    bootstrapServers = "192.168.99.100:9092",
    topic = "lowercaseStrings",
    groupId = "groupName",
    valueDeserializer = new StringDeserializer()
  ))

  val subscriber: Subscriber[StringProducerMessage] = kafka.publish(ProducerProperties(
    bootstrapServers = "192.168.99.100:9092",
    topic = "prova",
    valueSerializer = new StringSerializer(),
    keySerializer = new ByteArraySerializer()
  ))

  Source(1 to 1000000)
    .buffer(10, OverflowStrategy.backpressure)
    .map(n => ProducerMessage(n.toString.getBytes, n.toString))
    .to(Sink.fromSubscriber(subscriber)).run()

}

object ConsumerExample extends App {

  implicit val actorSystem = ActorSystem("ReactiveKafka")
  implicit val materializer = ActorMaterializer()

  implicit val executor = actorSystem.dispatcher

  val kafka = new ReactiveKafka()

  val publisher: Publisher[StringConsumerRecord] = kafka.consume(ConsumerProperties(
    bootstrapServers = "192.168.99.100:9092",
    topic = "prova",
    groupId = "groupName",
    valueDeserializer = new StringDeserializer()
  ))

  val subscriber: Subscriber[StringProducerMessage] = kafka.publish(ProducerProperties(
    bootstrapServers = "192.168.99.100:9092",
    topic = "prova1",
    valueSerializer = new StringSerializer(),
    keySerializer = new ByteArraySerializer()
  ))

  Source.fromPublisher(publisher)
    .buffer(10, OverflowStrategy.backpressure)
    .map(n => ProducerMessage(n.toString.getBytes, n.toString))
    .to(Sink.fromSubscriber(subscriber)).run()

}
package test

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.kafka.scaladsl._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, ByteArrayDeserializer, StringSerializer, ByteArraySerializer}

/**
  * Created by fabiofumarola on 25/04/16.
  */
object ReactiveKafkaProducer extends App{

  implicit val system = ActorSystem("reactive-kafka")
  implicit val materializer = ActorMaterializer()
  implicit val executor = system.dispatcher

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("192.168.99.100:9092")

  Source(1 to 10000)
    .map(_.toString).log("test")
    .map(elem => new ProducerRecord[Array[Byte], String]("topic1", elem))
    .to(Producer.plainSink(producerSettings)).run()
}

object ReactiveKafkaConsumer extends App{

  implicit val system = ActorSystem("reactive-kafka")
  implicit val materializer = ActorMaterializer()
  implicit val executor = system.dispatcher


  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer,
    Set("topic1"))
    .withBootstrapServers("192.168.99.100:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  Consumer.plainSource(consumerSettings)
    .runWith(Sink.foreach(println))
}

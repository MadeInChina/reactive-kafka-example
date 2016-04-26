package org.hrw.reactive.kafka.example

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.{Committable, CommittableMessage}
import akka.kafka.scaladsl.{Producer, Consumer}
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.stream.{SinkShape, ClosedShape, ActorMaterializer}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.concurrent.Future


object ConsumerExample extends App {
  implicit val system = ActorSystem("ReactiveKafka")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer, Set.empty[String] /*Set("topic1")*/)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val settings =  consumerSettings.withAssignment(new TopicPartition("test", 0))

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")

//  Consumer.committableSource(settings.withClientId("client1"))
//    .map(msg => println(msg.value))
//    .runWith(Sink.ignore)

  Consumer.committableSource(settings.withClientId("client1"))
    .map(msg =>{
      println(msg.value)
      Producer.Message(new ProducerRecord[Array[Byte], String]("test", msg.value), msg.committableOffset)
    })
    .to(Producer.commitableSink(producerSettings)).run()
}

object ConsumerGraphExample extends App{

  implicit val system = ActorSystem("ReactiveKafka")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer, Set.empty[String] /*Set("topic1")*/)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
  val id = "1"
  val num = 10
  var count = 0
  def reset() = count = 0
  def syncInc() = this.synchronized {
    count += 1
    println(s"[$id] Time ($count): " + (count / ((System.currentTimeMillis() - now) / 1000)) + " TPS")
  }
  val settings =  consumerSettings.withAssignment(new TopicPartition("test", 0))

  val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[akka.NotUsed] =>
    import GraphDSL.Implicits._
    type In = Consumer.CommittableMessage[Array[Byte], String]

    val src = Consumer.committableSource(settings.withClientId("client" + id))
    val commit = Flow[In].mapAsync(1) { msg =>

      Future {

        syncInc()
        Producer.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset)
      }
    }
    val work = Flow[In].map { i => i } // a dummy step where real "work" would happen

    src ~> work ~> commit ~> Producer.commitableSink(producerSettings)
    ClosedShape
  })
  val now = System.currentTimeMillis()
  graph.run()
}
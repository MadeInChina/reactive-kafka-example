package org.hrw.reactive.kafka.example

import java.util.concurrent.LinkedBlockingQueue

import akka.actor.{Actor, ActorSystem, Props}
import akka.kafka.scaladsl.Consumer.CommittableOffset
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy, RequestStrategy}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future


object ConsumerExample extends App {
  implicit val system = ActorSystem("ReactiveKafka")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer, Set.empty[String] /*Set("topic1")*/)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val settings = consumerSettings.withAssignment(new TopicPartition("test", 0))

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")

  //  Consumer.committableSource(settings.withClientId("client1"))
  //    .map(msg => println(msg.value))
  //    .runWith(Sink.ignore)

  Consumer.committableSource(settings.withClientId("client1"))
    .map(msg => {
      println(msg.value)
      Producer.Message(new ProducerRecord[Array[Byte], String]("test", msg.value), msg.committableOffset)
    })
    .to(Producer.commitableSink(producerSettings)).run()
}

object ConsumerGraphExample extends App {

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
  val settings = consumerSettings.withAssignment(new TopicPartition("test", 0))
  val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[akka.NotUsed] =>
    import GraphDSL.Implicits._
    type In = Consumer.CommittableMessage[Array[Byte], String]

    val src = Consumer.committableSource(settings.withClientId("client" + id))
    src.buffer(1000, OverflowStrategy.backpressure)
    val commit = Flow[In].mapAsync(1) { msg =>

      Future {

        syncInc()
        Producer.Message(new ProducerRecord[Array[Byte], String]("test", msg.value), msg.committableOffset)
      }
    }
    val work = Flow[In].map { i => i } // a dummy step where real "work" would happen

    src ~> work ~> commit ~> Producer.commitableSink(producerSettings)
    ClosedShape
  })
  val now = System.currentTimeMillis()
  var count = 0

  def reset() = count = 0

  def syncInc() = this.synchronized {
    count += 1
    println(s"[$id] Time ($count): " + (count / ((System.currentTimeMillis() - now) / 1000)) + " TPS")
  }

  graph.run()
}

object KafkaStream extends App {
  type In = Consumer.CommittableMessage[Array[Byte], String]
  implicit val system = ActorSystem("ReactiveKafka")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer, Set.empty[String] /*Set("topic1")*/)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest").withAssignment(new TopicPartition("test", 0))
  val src = Consumer.committableSource(consumerSettings.withClientId("client-test"))
  src.buffer(1000, OverflowStrategy.backpressure)

  val now = System.currentTimeMillis()
  var count = 0
  def syncInc() = this.synchronized {
    count += 1
    println(s"Time ($count): " + (count / ((System.currentTimeMillis() - now) / 1000)) + " TPS")
  }

  val finalConsumer = system.actorOf(Props(new Actor {

    val proxy = StreamConsumerProxy.props

    val commit = Flow[In].map { msg =>
      println("commit msg:" + msg)
      DataWithOffset(msg.value, msg.committableOffset)
    }.buffer(1, OverflowStrategy.backpressure)
//    val commit = Flow[In].mapAsync(1) { msg =>
//
//      Future {
//
//        syncInc()
//        DataWithOffset(msg.value, msg.committableOffset)
//      }
//    }.buffer(1, OverflowStrategy.backpressure)

    val t = src.via(commit).runWith(Sink.actorSubscriber(
      proxy))

    override def receive: Actor.Receive = {

      case Pulled(list) =>
        println("pulled data:" + list)
        list.foreach(d => d.offset.commit())

      case Pull(amount) =>
        t ! Pull(1)
      case a@_ =>
        println("Ignore" + a)
    }
  }))
  Thread.sleep(1001)
  finalConsumer ! Pull(1)
  finalConsumer ! Pull(1)
  finalConsumer ! Pull(1)
  finalConsumer ! Pull(1)

}

object StreamConsumerProxy {
  def props(): Props = Props(new StreamConsumerProxy())
}

class StreamConsumerProxy() extends ActorSubscriber {
  private val buffer = new LinkedBlockingQueue[DataWithOffset](10000)


  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    println("start")
  }

  override def receive: Actor.Receive = {
    case OnNext(msg: DataWithOffset) =>
      println("OnNext:" + msg)
      buffer.put(msg)
    case Pull(amount) =>
      println("Pull")
      val result = ArrayBuffer[DataWithOffset]()
      while (buffer.size() > 0 && result.size < amount) {
        buffer.poll match {
          case null => //do nothing if data not correct
          case data => result += data
        }
      }
      sender() ! Pulled(result.toList)
  }

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(max = 100000) {
    override def inFlightInternally: Int = buffer.size
  }
}

case class Pull(amount: Int)

case class Pulled(data: List[DataWithOffset])

case class DataWithOffset(data: String, offset: CommittableOffset)


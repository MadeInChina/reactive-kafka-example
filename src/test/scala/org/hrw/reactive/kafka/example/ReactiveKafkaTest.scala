package org.hrw.reactive.kafka.example
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.KafkaMessages.StringKafkaMessage
import kafka.serializer.{StringDecoder, StringEncoder}
import org.reactivestreams.{Publisher, Subscriber}
import com.softwaremill.react.kafka.{ReactiveKafka, ProducerProperties, ConsumerProperties}
class ReactiveKafkaTest {
  implicit val actorSystem = ActorSystem("ReactiveKafka")
  implicit val materializer = ActorMaterializer()

  val kafka = new ReactiveKafka()
  val publisher: Publisher[StringKafkaMessage] = kafka.consume(ConsumerProperties(
    brokerList = "localhost:9092",
    zooKeeperHost = "localhost:2181",
    topic = "lowercaseStrings",
    groupId = "groupName",
    decoder = new StringDecoder()
  ))
  val subscriber: Subscriber[String] = kafka.publish(ProducerProperties(
    brokerList = "localhost:9092",
    topic = "uppercaseStrings",
    encoder = new StringEncoder()
  ))

  Source.fromPublisher(publisher).map(_.message().toUpperCase)
    .to(Sink.fromSubscriber(subscriber)).run()
}

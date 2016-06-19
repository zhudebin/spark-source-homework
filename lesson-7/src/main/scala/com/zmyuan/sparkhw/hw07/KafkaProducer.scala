package com.zmyuan.sparkhw.hw07

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

/**
  * Created by zdb on 2016/6/5.
  */
class KafkaProducer {
  import KafkaProducer._

  def send(key:String, value: String): Unit = {
    producer.send(new KeyedMessage[String, String]("test", key, value))
  }

}

object KafkaProducer {
  val producer: Producer[String, String] = {
    val props: Properties = new Properties
    props.setProperty("metadata.broker.list", "192.168.0.90:9092")
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    val config: ProducerConfig = new ProducerConfig(props)
    new Producer[String, String](config)
  }
}

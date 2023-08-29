package ThreeScenarios

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object KafkaProducerApp {
  def main(args: Array[String]): Unit = {

    // Configure Kafka producer properties
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Create a Kafka producer instance
    val producer = new KafkaProducer[String, String](props)

    // Define the topic to which I want to send messages
    val topic = "BDAT1008_01"

    // Create value for the Kafka message
    val messages = List(
      "This is Group 01.",
      "It is our Third scenario.",
      "We have successfully implemented all scenarios. "
    )

    // Create a ProducerRecord with the topic and value
    messages.foreach { message =>
      val record = new ProducerRecord[String, String](topic, message)
      producer.send(record)
    }

    // Close the producer instance
    producer.close()
  }
}

package ThreeScenarios

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.Properties
import java.time.Duration
import scala.collection.JavaConverters._

object KafkaConsumerApp {
  def main(args: Array[String]): Unit = {

    // Configure Kafka consumer properties
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group")

    // Create a Kafka consumer instance
    val consumer = new KafkaConsumer[String, String](props)

    // Define the topic from which I want to consume messages
    val topic = "BDAT1008_01"

    // Subscribe to the topic
    consumer.subscribe(java.util.Collections.singletonList(topic))

    val messageCountToConsume = 10
    var messagesConsumed = 0

    while (messagesConsumed < messageCountToConsume) {
      // Poll records from Kafka with a timeout of 100 milliseconds
      val records = consumer.poll(Duration.ofMillis(100))
      for (record <- records.asScala) {
        // Print the consumed message
        println(s"Received message: ${record.value}")
        messagesConsumed += 1
      }
    }

    // Close the consumer instance
    consumer.close()
  }
}



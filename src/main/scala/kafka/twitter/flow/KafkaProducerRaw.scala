package kafka.twitter.flow

import java.io.{BufferedWriter, File, FileWriter}
import java.util.{Calendar, Date, Properties}

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.KafkaException
import org.apache.log4j.Logger

object KafkaProducerRaw {
  var logger = Logger.getLogger(this.getClass.getName)

  val props = new Properties()
  props.put("bootstrap.servers", "192.168.56.101:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("auto_offset_reset", "earliest_offset")
  props.put("enable_auto_commit", "true")
  val TOPIC = "test_1"

  def sendRecordToKafka(createdAt: String, hashTag: String, message: String): Unit = {
    logger.info("Tweet Received")
    println("Tweet Received")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord(TOPIC, createdAt, hashTag + "\t" + message)
    producer.send(record, new Callback() {
      def onCompletion(recordMetadata: RecordMetadata, exception: Exception): Unit = {
        if (recordMetadata == null) {
          throw new KafkaException("Message not acknowledged")
        }
        else
          println("Tweet Pushed to Kafka")
      }
    }
    )
    logger.info("Tweet created at "+createdAt+" is pushed successfully to kafka ")
   // println("Tweet created at " + createdAt + " is pushed successfully to kafka ")
    producer.close()
  }
}
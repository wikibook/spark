package com.wikibooks.spark.ch6.scala

import kafka.serializer.StringDecoder
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object KafkaSample {

  def main(args: Array[String]) {
    
    import org.apache.spark.streaming.kafka._

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("KafkaSample")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    val zkQuorum = "localhost:2181"
    val groupId = "test-consumer-group1"
    val topics = Map("test" -> 3)
    
    val ds1 = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
    val ds2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, 
        Map("metadata.broker.list" -> "localhost:9092"), 
        Set("test"))

    ds1.print
    ds2.print

    ssc.start
    ssc.awaitTermination()
  }
}
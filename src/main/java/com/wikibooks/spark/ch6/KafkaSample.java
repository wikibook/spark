package com.wikibooks.spark.ch6;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class KafkaSample {

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("KafkaSample");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(3));

    Map<String, Integer> topics1 = new HashMap<>();
    topics1.put("test", 3);

    Map<String, String> params = new HashMap<>();
    params.put("metadata.broker.list", "localhost:9092");

    Set<String> topics2 = new HashSet<>();
    topics2.add("test");

    JavaPairReceiverInputDStream<String, String> ds1 = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group1", topics1);
    JavaPairInputDStream<String, String> ds2 = KafkaUtils.<String, String, StringDecoder, StringDecoder> createDirectStream(ssc, 
        String.class, 
        String.class, 
        StringDecoder.class, 
        StringDecoder.class, 
        params, 
        topics2);

    ds1.print();
    ds2.print();

    ssc.start();
    ssc.awaitTermination();
  }
}
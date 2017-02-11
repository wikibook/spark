package com.wikibooks.spark.ch6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

// 6.2.1절 예제 6-5
public class SocketSample {

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("SocketSample");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(3));

    JavaReceiverInputDStream<String> ds =  ssc.socketTextStream("localhost", 9000);

    ds.print();
    
    ssc.start();
    ssc.awaitTermination();
  }
}
package com.wikibooks.spark.ch6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

// 6.2.2절 예제 6-8
public class FileSample {

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("FileSample");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(3));

    JavaDStream<String> ds = ssc.textFileStream("file:///Users/beginspark/Temp");

    ds.print();

    ssc.start();
    ssc.awaitTermination();
  }
}

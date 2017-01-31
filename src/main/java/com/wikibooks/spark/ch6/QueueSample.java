package com.wikibooks.spark.ch6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class QueueSample {

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("QueueSample");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));

    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("c", "d", "e"));
    Queue<JavaRDD<String>> queue = new LinkedList<>(Arrays.asList(rdd1, rdd2));

    JavaDStream<String> ds = ssc.queueStream(queue);

    ds.print();

    ssc.start();
    ssc.awaitTermination();
  }
}
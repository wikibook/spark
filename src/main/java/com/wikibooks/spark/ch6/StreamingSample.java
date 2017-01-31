package com.wikibooks.spark.ch6;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingSample {

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("StreamingSample");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(3));

    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("Spark Streaming Sample ssc"));
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("Spark Quque Spark API"));
    Queue<JavaRDD<String>> inputQueue = new LinkedList<>(Arrays.asList(rdd1, rdd2));
    JavaInputDStream<String> lines = ssc.queueStream(inputQueue, true);

    // Java7
    JavaDStream words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String v) throws Exception {
        return Arrays.asList(v.split(" ")).iterator();
      }
    });

    // Java8
    JavaDStream words2 = lines.flatMap((String v) -> Arrays.stream(v.split(" ")).iterator());

    words.countByValue().print();

    ssc.start();
    ssc.awaitTermination();
  }
}
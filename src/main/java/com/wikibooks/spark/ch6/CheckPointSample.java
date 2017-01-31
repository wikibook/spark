package com.wikibooks.spark.ch6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;

public class CheckPointSample {

  public static JavaStreamingContext createSSC(String checkpointDir) {

    // ssc 생성
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("CheckPointSample");
    JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));
    JavaSparkContext sc = ssc.sparkContext();

    // checkpoint
    ssc.checkpoint(checkpointDir);

    // DStream 생성
    JavaDStream<String> ids1 = ssc.socketTextStream("localhost", 9000);

    // Java7
    JavaPairDStream<String, Long> ids2 = ids1.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
      }
    }).mapToPair(new PairFunction<String, String, Long>() {
      @Override
      public Tuple2<String, Long> call(String s) throws Exception {
        return new Tuple2<String, Long>(s, 1L);
      }
    });

    // Java8
    JavaPairDStream<String, Long> ids2_2 = ids1.flatMap((String s) -> Arrays.asList(s.split(" ")).iterator())
            .mapToPair((String s) -> new Tuple2<String, Long>(s, 1L));

    // updateStateByKey(Java7)
    ids2.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
      @Override
      public Optional<Long> call(List<Long> newValues, Optional<Long> currentValue) throws Exception {
        Long sum = 0L;
        for (long v : newValues) sum += v;
        return Optional.of(currentValue.orElse(0L) + sum);
      }
    }).print();

    // updateStateByKey(Java8)
    ids2.updateStateByKey((List<Long> newValues, Optional<Long> currentValue) -> {
      Long sum = 0L;
      for (long v : newValues) sum += v;
      return Optional.of(currentValue.orElse(0L) + sum);
    }).print();

    // return
    return ssc;
  }

  public static void main(String[] args) throws Exception {
    String checkpointDir = "./checkPoints/CheckPointSample/Java";
    JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDir, new Function0<JavaStreamingContext>() {
      @Override
      public JavaStreamingContext call() throws Exception {
        return createSSC(checkpointDir);
      }
    });
    ssc.start();
    ssc.awaitTermination();
  }
}
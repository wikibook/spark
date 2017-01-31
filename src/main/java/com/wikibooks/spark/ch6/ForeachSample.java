package com.wikibooks.spark.ch6;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;

public class ForeachSample {

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("ForeachSample");
    JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
    JavaSparkContext sc = ssc.sparkContext();

    JavaRDD<Person> rdd1 = sc.parallelize(Arrays.asList(new Person("P1", 20)));
    JavaRDD<Person> rdd2 = sc.parallelize(Arrays.asList(new Person("P2", 10)));

    Queue<JavaRDD<Person>> queue = new LinkedList<>(Arrays.asList(rdd1, rdd2));
    JavaDStream<Person> ds = ssc.queueStream(queue);

    ds.foreachRDD(new VoidFunction<JavaRDD<Person>>() {
      @Override
      public void call(JavaRDD<Person> rdd) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("Sample")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = spark.createDataFrame(rdd, Person.class);
        df.select("name", "age").show();
      }
    });

    ssc.start();
    ssc.awaitTermination();
  }
}
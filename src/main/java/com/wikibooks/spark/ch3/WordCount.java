package com.wikibooks.spark.ch3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

  private static void run(String inputPath, String outputPath) {

    SparkConf conf = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> rdd1 = sc.textFile(inputPath);

    JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String v) throws Exception {
        return Arrays.asList(v.split(" ")).iterator();
      }
    });

    JavaPairRDD<String, Long> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Long>() {
      @Override
      public Tuple2<String, Long> call(String s) throws Exception {
        return new Tuple2<String, Long>(s, 1L);
      }
    });

    JavaPairRDD<String, Long> rdd4 = rdd3.reduceByKey(new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    });

    rdd4.saveAsTextFile(outputPath);

    sc.stop();
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 2) {
      run(args[0], args[1]);
    } else {
      System.out.println("Usage: $SPARK_HOME/bin/spark-submit --class <class_name> --master <master> --<option> <option_value> <jar_file_path> <input_path> <output_path>");
    }
  }
}

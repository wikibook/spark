package com.wikibooks.spark.ch2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class RDDCreateSample {

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("RDDCreateSample");

    JavaSparkContext sc = new JavaSparkContext(conf);

    // 2.1.3절 예제 2-6
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));

    // 2.1.3절 예제 2-8
    JavaRDD<String> rdd2 = sc.textFile("<spark_home_dir>/README.md");

    for (String v : rdd1.collect()) {
      System.out.println(v);
    }

    for (String v : rdd2.collect()) {
      System.out.println(v);
    }

    sc.stop();
  }
}
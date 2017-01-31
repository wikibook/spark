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

    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));

    // <project_home>은 예제 프로젝트의 경로
    JavaRDD<String> rdd2 = sc.textFile("file://<project_home>/source/data/sample.txt");

    for (String v : rdd1.collect()) {
      System.out.println(v);
    }

    for (String v : rdd2.collect()) {
      System.out.println(v);
    }

    sc.stop();
  }
}
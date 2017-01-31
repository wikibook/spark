package com.wikibooks.spark.ch1;

import com.wikibooks.spark.ch1.WordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

// 1.4.1절
public class WordCountTest {

  private static SparkConf conf;
  private static JavaSparkContext sc;

  @BeforeClass
  public static void setup() {
    conf = new SparkConf().setAppName("WordCountTest").setMaster("local[*]");
    sc = new JavaSparkContext(conf);
  }

  @Test
  public void testProcess() {

    List<String> input = new ArrayList<String>();
    input.add("Apache Spark is a fast and general engine for large-scale data processing.");
    input.add("Spark runs on both Windows and UNIX-like systems");

    JavaRDD<String> inputRDD = sc.parallelize(input);

    JavaPairRDD<String, Integer> resultRDD = WordCount.process(inputRDD);

    Map<String, Integer> resultMap = resultRDD.collectAsMap();

    assertThat(2, is(resultMap.get("Spark")));
    assertThat(2, is(resultMap.get("and")));
    assertThat(1, is(resultMap.get("runs")));

    System.out.println(resultMap);
  }

  @AfterClass
  public static void cleanup() {
    if (sc != null) {
      sc.stop();
    }
  }
}
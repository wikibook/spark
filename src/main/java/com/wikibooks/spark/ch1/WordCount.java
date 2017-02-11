package com.wikibooks.spark.ch1;

import org.apache.commons.lang3.ArrayUtils;
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

// 1.4.1절
public class WordCount {

  public static void main(String[] args) {

    if (ArrayUtils.getLength(args) != 3) {
      System.out.println("Usage: WordCount <Master> <Input> <Output>");
      return;
    }

    // Step1: SparkContext 생성
    JavaSparkContext sc = getSparkContext("WordCount", args[0]);

    try {
      // Step2: 입력 소스로부터 RDD 생성
      JavaRDD<String> inputRDD = getInputRDD(sc, args[1]);

      // Step3: 필요한 처리를 수행
      JavaPairRDD<String, Integer> resultRDD = process(inputRDD);

      // Step4: 수행 결과 처리
      handleResult(resultRDD, args[2]);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // Step5: Spark와의 연결 종료
      sc.stop();
    }
  }

  public static JavaSparkContext getSparkContext(String appName, String master) {
    SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
    return new JavaSparkContext(conf);
  }

  public static JavaRDD<String> getInputRDD(JavaSparkContext sc, String input) {
    return sc.textFile(input);
  }

  // Java7
  public static JavaPairRDD<String, Integer> process(JavaRDD<String> inputRDD) {

    JavaRDD<String> words = inputRDD.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
      }
    });

    JavaPairRDD<String, Integer> wcPair = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) throws Exception {
        return new Tuple2(s, 1);
      }
    });

    JavaPairRDD<String, Integer> result = wcPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });

    return result;
  }

  // Java8 (Lambda)
  public static JavaPairRDD<String, Integer> processWithLambda(JavaRDD<String> inputRDD) {

    JavaRDD<String> words = inputRDD.flatMap((String s) -> Arrays.asList(s.split(" ")).iterator());

    JavaPairRDD<String, Integer> wcPair = words.mapToPair((String w) -> new Tuple2(w, 1));

    JavaPairRDD<String, Integer> result = wcPair.reduceByKey((Integer c1, Integer c2) -> c1 + c2);

    return result;
  }

  public static void handleResult(JavaPairRDD<String, Integer> resultRDD, String output) {
    resultRDD.saveAsTextFile(output);
  }
}
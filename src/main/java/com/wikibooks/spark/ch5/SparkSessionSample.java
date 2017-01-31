package com.wikibooks.spark.ch5;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

import static org.apache.spark.sql.functions.*;

public class SparkSessionSample {

  public static void main(String[] args) {

    // ex5-2, ex5-13
    SparkSession spark = SparkSession
            .builder()
            .appName("SparkSessionSample")
            .master("local[*]")
            .getOrCreate();

    // ex5-5
    String source = "file:///Users/beginspark/Apps/spark/README.md";
    Dataset<Row> df = spark.read().text(source);

    // ex5-8
    runUntypedTransformationsExample(spark, df);

    runTypedTransformationsExample(spark, df);

    spark.stop();
  }

  // ex5-8
  public static void runUntypedTransformationsExample(SparkSession spark, Dataset<Row> df) {
    Dataset<Row> wordDF = df.select(explode(split(col("value"), " ")).as("word"));
    Dataset<Row> result = wordDF.groupBy("word").count();
    result.show();
  }

  // ex5-11
  public static void runTypedTransformationsExample(SparkSession spark, Dataset<Row> df) {

    Dataset<String> ds = df.as(Encoders.STRING());

    // Java7
    Dataset<String> wordDF = ds.flatMap(new FlatMapFunction<String, String>() {

      @Override
      public Iterator<String> call(String v) throws Exception {
        return Arrays.asList(v.split(" ")).iterator();
      }

    }, Encoders.STRING());

    Dataset<Tuple2<String, Object>> result = wordDF.groupByKey(new MapFunction<String, String>() {

      @Override
      public String call(String value) throws Exception {
        return value;
      }

    }, Encoders.STRING()).count();

    // Java8
    Dataset<String> wordDF2 = ds.flatMap((String v) -> Arrays.asList(v.split(" ")).iterator(), Encoders.STRING());

    // 일부 IDE에서 아래 groupByKey를 구문 오류로 인식하는 경우가 있으나 컴파일 및 동작에 영향 없음
    //Dataset<Tuple2<String, Object>> result2 = wordDF2.groupByKey((String value) -> value, Encoders.STRING()).count();

    result.show();
  }
}
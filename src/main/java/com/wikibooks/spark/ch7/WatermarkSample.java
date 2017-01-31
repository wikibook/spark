package com.wikibooks.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.*;

// 7.4.3절
public class WatermarkSample {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
            .builder()
            .appName("WatermarkSample")
            .master("local[*]")
            .getOrCreate();

    // port번호는 ncat 서버와 동일하게 수정해야 함
    Dataset<Row> lines = spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 9000)
            .option("includeTimestamp", true)
            .load()
            .toDF("words", "timestamp");

    Dataset<Row> words = lines.select(explode(split(col("words"), " ")).as("word"), col("timestamp"));

    // 빠른 결과 확인을 위해서 워터마크 및 윈도우 주기를 초단위로 설정하였음 (실제로는 데이터 처리 기준에 맞춰 분 또는 시간 단위로 설정)
    Dataset<Row> wordCount = words.withWatermark("timestamp", "5 seconds").groupBy(window(col("timestamp"), "10 seconds", "5 seconds"), col("word")).count();

    // 완료모드 사용시 오래된 집계결과를 지우지 않음
    StreamingQuery query = wordCount.writeStream()
            .outputMode(OutputMode.Append())
            .format("console")
            .option("truncate", false)
            .start();

    query.awaitTermination();
  }
}
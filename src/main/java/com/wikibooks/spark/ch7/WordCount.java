package com.wikibooks.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.*;

public class WordCount {

  // 7.2절
  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
            .builder()
            .appName("WordCount")
            .master("local[*]")
            .getOrCreate();

    // port번호는 ncat 서버와 동일하게 수정해야 함
    Dataset<Row> lines = spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 9000)
            .load();

    Dataset<Row> words = lines.select(explode(split(col("value"), " ")).as("word"));
    Dataset<Row> wordCount = words.groupBy("word").count();

    StreamingQuery query = wordCount.writeStream()
            .outputMode(OutputMode.Complete())
            .format("console")
            .start();

    query.awaitTermination();
  }
}

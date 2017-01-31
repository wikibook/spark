package com.wikibooks.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import static org.apache.spark.sql.functions.*;

// 7.4.2절
public class WordCountByWindow {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
        .builder()
        .appName("WordCountByWindow")
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
    .toDF("words", "ts");
    
    Dataset<Row> words = lines.select(explode(split(col("words"), " ")).as("word"), window(col("ts"), "10 minute", "5 minute").as("time"));
    Dataset<Row> wordCount = words.groupBy("time", "word").count();
    
    StreamingQuery query = wordCount.writeStream()
      .outputMode(OutputMode.Complete())
      .option("truncate", false)
      .format("console")
      .start();

    query.awaitTermination();    
  }
}

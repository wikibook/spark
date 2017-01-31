package com.wikibooks.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.*;

public class ForeachSample {

  // 7.2절
  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
            .builder()
            .appName("ForeachSample")
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

    // foreach 메서드는 스칼라 및 자바만 사용 가능
    StreamingQuery query = wordCount
            .writeStream()
            .outputMode(OutputMode.Complete())
            .foreach(new ForeachWriter<Row>() {
              @Override
              public boolean open(long partitionId, long version) {
                System.out.println("partitionId:" + partitionId + "version:" + version);
                return true;
              }

              @Override
              public void process(Row value) {
                System.out.println("process:" + value.mkString(", "));
              }

              @Override
              public void close(Throwable errorOrNull) {
                System.out.println("close");
              }
            }).start();

    query.awaitTermination();
  }
}

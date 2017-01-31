package com.wikibooks.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

// 7.3절
public class StructuredStreamSample {

  private static void runReadStreamSample(SparkSession spark) throws Exception {

    // 예제를 실행 후
    // "<spark_home>/examples/src/main/resources/peple.json" 파일을
    // json("..") 메서드에 지정한 디렉토리에 복사하여 결과 확인
    Dataset<Row> source = spark
            .readStream()
            .schema(new StructType().add("name", "string").add("age", "integer"))
            .json("/Users/beginspark/Temp/json");

    Dataset<Row> result = source.groupBy("name").count();

    result.writeStream()
            .outputMode(OutputMode.Complete())
            .format("console")
            .start()
            .awaitTermination();
  }


  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
            .builder()
            .appName("StructuredStreamSample")
            .master("local[*]")
            .getOrCreate();

    runReadStreamSample(spark);

  }
}

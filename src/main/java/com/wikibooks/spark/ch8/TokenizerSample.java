package com.wikibooks.spark.ch8;

import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class TokenizerSample {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession.builder()
            .appName("TokenizerSample")
            .master("local[*]")
            .getOrCreate();

    StructField sf1 = DataTypes.createStructField("input", DataTypes.StringType, true);
    StructType st1 = DataTypes.createStructType(Arrays.asList(sf1));
    Row r1 = RowFactory.create("Tokenization is the process");
    Row r2 = RowFactory.create("Refer to the Tokenizer");

    Dataset<Row> inputDF = spark.createDataFrame(Arrays.asList(r1, r2), st1);
    Tokenizer tokenizer = new Tokenizer().setInputCol("input").setOutputCol("output");
    Dataset<Row> outputDF = tokenizer.transform(inputDF);
    outputDF.printSchema();
    outputDF.show(false);

    spark.stop();
  }
}

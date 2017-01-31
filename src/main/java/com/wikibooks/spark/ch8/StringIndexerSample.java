package com.wikibooks.spark.ch8;

import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class StringIndexerSample {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession.builder()
            .appName("StringIndexerSample")
            .master("local[*]")
            .getOrCreate();

    StructField sf1 = DataTypes.createStructField("id", DataTypes.IntegerType, true);
    StructField sf2 = DataTypes.createStructField("color", DataTypes.StringType, true);
    StructType st1 = DataTypes.createStructType(Arrays.asList(sf1, sf2));

    Row r1 = RowFactory.create(0, "red");
    Row r2 = RowFactory.create(1, "blue");
    Row r3 = RowFactory.create(2, "green");
    Row r4 = RowFactory.create(3, "yellow");

    Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(r1, r2, r3, r4), st1);

    StringIndexerModel strignIndexer = new StringIndexer()
            .setInputCol("color")
            .setOutputCol("colorIndex")
            .fit(df1);

    Dataset<Row> df2 = strignIndexer.transform(df1);

    df2.show(false);

    IndexToString indexToString = new IndexToString()
            .setInputCol("colorIndex")
            .setOutputCol("originalColor");

    Dataset<Row> df3 = indexToString.transform(df2);
    df3.show(false);

    spark.stop();
  }
}
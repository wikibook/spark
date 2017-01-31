package com.wikibooks.spark.ch8;

import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class TfIDFSample {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession.builder()
            .appName("TfIDFSample")
            .master("local[*]")
            .getOrCreate();

    StructField sf1 = DataTypes.createStructField("label", DataTypes.IntegerType, true);
    StructField sf2 = DataTypes.createStructField("sentence", DataTypes.StringType, true);
    StructType st1 = DataTypes.createStructType(Arrays.asList(sf1, sf2));

    Row r1 = RowFactory.create(0, "a a a b b c");
    Row r2 = RowFactory.create(0, "a b c");
    Row r3 = RowFactory.create(1, "a c a a d");

    Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(r1, r2, r3), st1);

    Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
    // 각 문장을 단어로 분리
    Dataset<Row> df2 = tokenizer.transform(df1);

    HashingTF hashingTF = new HashingTF()
            .setInputCol("words").setOutputCol("TF-Features").setNumFeatures(20);

    Dataset<Row> df3 = hashingTF.transform(df2);

    df3.cache();

    IDF idf = new IDF().setInputCol("TF-Features").setOutputCol("Final-Features");
    IDFModel idfModel = idf.fit(df3);

    Dataset<Row> rescaledData = idfModel.transform(df3);
    rescaledData.select("words", "TF-Features", "Final-Features").show(false);

    spark.stop();
  }
}

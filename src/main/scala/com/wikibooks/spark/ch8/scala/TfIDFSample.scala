package com.wikibooks.spark.ch8.scala

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

object TfIDFSample {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("TfIDFSample")
      .master("local[*]")
      .getOrCreate()

    val df1 = spark.createDataFrame(Seq(
      (0, "a a a b b c"),
      (0, "a b c"),
      (1, "a c a a d"))).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    // 각 문장을 단어로 분리 
    val df2 = tokenizer.transform(df1)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("TF-Features").setNumFeatures(20)
    val df3 = hashingTF.transform(df2)

    df3.cache()

    val idf = new IDF().setInputCol("TF-Features").setOutputCol("Final-Features")
    val idfModel = idf.fit(df3)

    val rescaledData = idfModel.transform(df3)
    rescaledData.select("words", "TF-Features", "Final-Features").show(false)

    spark.stop
  }
}
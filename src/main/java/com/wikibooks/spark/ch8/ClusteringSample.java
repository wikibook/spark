package com.wikibooks.spark.ch8;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class ClusteringSample {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession.builder()
            .appName("ClusteringSample")
            .master("local[*]")
            .getOrCreate();

    // 원본데이터
    // http://data.seoul.go.kr/openinf/sheetview.jsp?infId=OA-13061&tMenu=11
    // 서울시 공공와이파이 위치정보 (영어)
    Dataset<Row> d1 = spark.read().option("header", "true")
            .option("sep", ",")
            .option("inferSchema", true)
            .option("mode", "DROPMALFORMED")
            .csv("/Users/beginspark/Temp/data3.csv");

    d1.printSchema();

    Dataset<Row> d2 = d1.toDF("number", "name", "SI", "GOO", "DONG", "x", "y", "b_code", "h_code", "utmk_x", "utmk_y", "wtm_x", "wtm_y");

    Dataset<Row> d3 = d2.select(d2.col("GOO").as("loc"), d2.col("x"), d2.col("y"));
    d3.show(5, false);

    StringIndexer indexer = new StringIndexer().setInputCol("loc").setOutputCol("loccode");

    VectorAssembler assembler = new VectorAssembler()
            .setInputCols(new String[]{"loccode", "x", "y"})
            .setOutputCol("features");

    KMeans kmeans = new KMeans().setK(5).setSeed(1L).setFeaturesCol("features");

    Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{indexer, assembler, kmeans});

    PipelineModel model = pipeline.fit(d3);

    Dataset<Row> d4 = model.transform(d3);

    d4.groupBy("prediction").agg(collect_set("loc").as("loc"))
            .orderBy("prediction").show(100, false);

    double WSSSE = ((KMeansModel) model.stages()[2]).computeCost(d4);
    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

    // Shows the result.
    System.out.println("Cluster Centers: ");

    for (Vector v : ((KMeansModel) model.stages()[2]).clusterCenters()) {
      System.out.println(v);
    }

    spark.stop();
  }
}














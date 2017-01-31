package com.wikibooks.spark.ch8;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function0;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class ClassficationSample {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession.builder()
            .appName("ClassficationSample")
            .master("local[*]")
            .getOrCreate();

    spark.udf().register("label", new UDF2<Double, Double, Double>() {
      @Override
      public Double call(Double avr_month, Double avr_total) throws Exception {
        return ((avr_month - avr_total) >= 0) ? 1.0d : 0.0d;
      }
    }, DataTypes.DoubleType);

    // 원본데이터
    // http://data.seoul.go.kr/openinf/sheetview.jsp?infId=OA-2604
    // 서울시 도시고속도로 월간 소통 통계
    Dataset<Row> d1 = spark.read().option("header", "true")
            .option("sep", ",").option("inferSchema", true)
            .option("mode", "DROPMALFORMED")
            .csv("/Users/beginspark/Temp/data2.csv");

    Dataset<Row> d2 = d1.toDF("year", "month", "road", "avr_traffic_month", "avr_velo_month", "mon", "tue", "wed", "thu", "fri", "sat", "sun");

    // data 확인
    d2.printSchema();

    // null 값 제거
    Dataset<Row> d3 = d2.where("avr_velo_month is not null");

    // 도로별 평균 속도
    Dataset<Row> d4 = d3.groupBy("road")
            .agg(round(avg("avr_velo_month"), 1).as("avr_velo_total"))
            .select(col("road").as("groad"), col("avr_velo_total"));

    Dataset<Row> d5 = d3.join(d4, d3.col("road").equalTo(d4.col("groad")));

//    // JavaConversions를 사용할 경우
//    Dataset<Row> d4 = d3.groupBy("road").agg(round(avg("avr_velo_month"), 1).as("avr_velo_total"));
//    Seq<String> joinCols = JavaConversions.asScalaBuffer(Arrays.asList("road")).toSeq();
//    Dataset<Row> d5 = d3.join(d4, joinCols);

    // label 부여
    Dataset<Row> d6 = d5.withColumn("label", callUDF("label", d5.col("avr_velo_month"), d5.col("avr_velo_total")));
    d6.select("road", "avr_velo_month", "avr_velo_total", "label").show(5, false);
    d6.groupBy("label").count().show(false);

    Dataset<Row>[] samples = d6.randomSplit(new double[]{0.7, 0.3});

    Dataset<Row> train = samples[0];
    Dataset<Row> test = samples[1];

    StringIndexer indexer = new StringIndexer().setInputCol("road").setOutputCol("roadcode");

    VectorAssembler assembler = new VectorAssembler()
            .setInputCols(new String[]{"roadcode", "mon", "tue", "wed", "thu", "fri", "sat", "sun"})
            .setOutputCol("features");

    DecisionTreeClassifier dt = new DecisionTreeClassifier()
            .setLabelCol("label")
            .setFeaturesCol("features");

    Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{indexer, assembler, dt});

    PipelineModel model = pipeline.fit(train);

    Dataset<Row> predict = model.transform(test);

    predict.select("label", "probability", "prediction").show(3, false);

    // areaUnderROC, areaUnderPR
    BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
            .setLabelCol("label")
            .setMetricName("areaUnderROC");

    System.out.println(evaluator.evaluate(predict));

    DecisionTreeClassificationModel treeModel = ((DecisionTreeClassificationModel) model.stages()[2]);
    System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());

    spark.stop();
  }
}














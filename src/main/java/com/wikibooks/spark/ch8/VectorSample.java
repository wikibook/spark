package com.wikibooks.spark.ch8;

import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.Collectors;

public class VectorSample {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession.builder()
            .appName("VectorSample")
            .master("local[*]")
            .getOrCreate();

    // 8.1.1절
    Vector v1 = Vectors.dense(0.1, 0.0, 0.2, 0.3);
    Vector v2 = Vectors.dense(new double[]{0.1, 0.0, 0.2, 0.3});
    Vector v3 = Vectors.sparse(4, Arrays.asList(new Tuple2(0, 0.1), new Tuple2(2, 0.2), new Tuple2(3, 0.3)));
    Vector v4 = Vectors.sparse(4, new int[]{0, 2, 3}, new double[]{0.1, 0.2, 0.3});

    System.out.println(Arrays.stream(v1.toArray())
            .mapToObj(String::valueOf).collect(Collectors.joining(", ")));

    System.out.println(Arrays.stream(v3.toArray())
            .mapToObj(String::valueOf).collect(Collectors.joining(", ")));

    // 8.1.2절
    LabeledPoint v5 = new LabeledPoint(1.0, v1);
    System.out.println("label:" + v5.label() + ", features:" + v5.features());

    String path = "file:///Users/beginspark/Apps/spark/data/mllib/sample_libsvm_data.txt";
    RDD<org.apache.spark.mllib.regression.LabeledPoint> v6
            = MLUtils.loadLibSVMFile(spark.sparkContext(), path);
    org.apache.spark.mllib.regression.LabeledPoint lp1 = v6.first();
    System.out.println("label:" + lp1.label() + ", features:" + lp1.features());

    spark.stop();
  }
}

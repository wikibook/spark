package com.wikibooks.spark.ch5;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

// 5.6절 하이브 연동
public class HiveSample {

  public static void main(String[] args) {

    SparkSession spark = SparkSession
            .builder()
            .appName("Spark Hive Example")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///Users/beginspark/hive/warehouse")
            .enableHiveSupport()
            .getOrCreate();

    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

    Person row1 = new Person("hayoon", 7, "student");
    Person row2 = new Person("sunwoo", 13, "student");
    Person row3 = new Person("hajoo", 5, "kindergartener");
    Person row4 = new Person("jinwoo", 13, "student");

    List<Person> data = Arrays.asList(row1, row2, row3, row4);
    Dataset<Person> ds = spark.createDataset(data, Encoders.bean(Person.class));

    spark.sql("CREATE TABLE IF NOT EXISTS Persons (name STRING, age INT, job STRING)");
    spark.sql("show tables").show();

    ds.toDF().write().mode(SaveMode.Overwrite).saveAsTable("Users");
    spark.sql("show tables").show();

    spark.stop();
  }
}

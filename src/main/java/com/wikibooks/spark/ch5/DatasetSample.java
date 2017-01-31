package com.wikibooks.spark.ch5;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.javalang.typed;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DatasetSample {

  // 5.6.1절
  public static void createDataSet(SparkSession spark, JavaSparkContext sc) {

    String sparkHomeDir = "/Users/beginspark/Apps/spark";

    // 파일로 부터 생성
    Dataset<String> ds1 = spark.read().textFile(sparkHomeDir + "/examples/src/main/resources/people.txt");

    // 자바 객체를 이용해 생성
    Person row1 = new Person("hayoon", 7, "student");
    Person row2 = new Person("sunwoo", 13, "student");
    Person row3 = new Person("hajoo", 5, "kindergartener");
    Person row4 = new Person("jinwoo", 13, "student");
    List<Person> data = Arrays.asList(row1, row2, row3, row4);
    Dataset<Person> ds2 = spark.createDataset(data, Encoders.bean(Person.class));

    // Encoder
    Encoder<Boolean> e1 = Encoders.BOOLEAN();
    Encoder<String> e2 = Encoders.STRING();
    Encoder<Person> e3 = Encoders.bean(Person.class);
    Encoder<Person> e4 = Encoders.javaSerialization(Person.class);
    Encoder<Person> e5 = Encoders.kryo(Person.class);

    // RDD를 이용해 생성
    JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3));
    Dataset<Integer> ds3 = spark.createDataset(javaRDD.rdd(), Encoders.INT());

    // 데이터프레임을 이용해 생성
    StructField f1 = DataTypes.createStructField("value", DataTypes.IntegerType, true);
    StructType schema = DataTypes.createStructType(Arrays.asList(f1));

    // Java8
    JavaRDD<Row> rows = javaRDD.map((Integer v) -> RowFactory.create(v));

    // Java7
    JavaRDD<Row> rows7 = javaRDD.map(new Function<Integer, Row>() {
      @Override
      public Row call(Integer v) throws Exception {
        return RowFactory.create(v);
      }
    });

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    Dataset<Integer> ds4 = df.as(Encoders.INT());

    // range()로 생성
    Dataset<Long> ds5 = spark.range(0, 10, 3);

    // 결과 확인
    ds1.show();
    ds2.show();
    ds3.show();
    ds4.show();
    ds5.show();

    // 참고(Dataset을 데이터프레임 및 RDD로 변환)
    ds5.toDF();
    ds5.toJavaRDD();
  }

  // 5.6.2.1절
  public static void runSelectEx(SparkSession spark, Dataset<Person> ds) {
    TypedColumn<Object, String> c1 = ds.col("name").as(Encoders.STRING());
    TypedColumn<Object, Integer> c2 = ds.col("age").as(Encoders.INT());
    ds.select(c1, c2).show();
  }

  // 5.6.2.2절
  public static void runAsEx(SparkSession spark) {
    Dataset<Long> d1 = spark.range(5, 15).as("First");
    Dataset<Long> d2 = spark.range(10, 20).as("Second");
    d1.join(d2, functions.expr("First.id = Second.id")).show();
  }

  // 5.6.2.3절
  public static void runDistinctEx(SparkSession spark) {
    List<Integer> data = Arrays.asList(1, 3, 3, 5, 5, 7);
    Dataset<Integer> ds = spark.createDataset(data, Encoders.INT());
    ds.distinct().show();
  }

  // 5.6.2.4절
  public static void runDropDuplicatesEx(SparkSession spark, Dataset<Person> ds) {
    // 원래 값
    ds.show();
    // 중복을 제외한 후
    ds.dropDuplicates("age").show();
  }

  // 5.6.2.5절
  public static void runFilterEx(SparkSession spark, Dataset<Person> ds) {
    // Java7
    Dataset<Person> rst7 = ds.filter(new FilterFunction<Person>() {
      @Override
      public boolean call(Person p) throws Exception {
        return p.getAge() < 10;
      }
    });
    rst7.show();
    // Java8
    Dataset<Person> rst8 = ds.filter((Person p) -> p.getAge() < 10);
    rst8.show();
  }

  // 5.6.2.6절
  public static void runFlatMap(SparkSession spark) {
    String sentence = "Spark SQL, DataFrames and Datasets Guide)";
    Dataset<String> ds = spark.createDataset(Arrays.asList(sentence), Encoders.STRING());

    // Java7
    Dataset<String> rst7 = ds.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
      }
    }, Encoders.STRING());
    rst7.show();

    // Java8
    Dataset<String> rst8 = ds.flatMap(
            (String s) -> Arrays.asList(s.split(" ")).iterator(),
            Encoders.STRING());
  }

  // 5.6.2.7절
  public static void runGroupByKeyEx(SparkSession spark, Dataset<Person> ds) {
    // Java7
    KeyValueGroupedDataset<Integer, Person> rst7 = ds.groupByKey(new MapFunction<Person, Integer>() {
      @Override
      public Integer call(Person p) throws Exception {
        return p.getAge();
      }
    }, Encoders.INT());

    // Java8
    KeyValueGroupedDataset<Integer, Person> rst8 = ds.groupByKey((MapFunction<Person, Integer>) (Person p) -> p.getAge(),
            Encoders.INT());

    rst7.count().show();
  }

  // 5.6.2.8절
  public static void runAgg(SparkSession spark, Dataset<Person> ds) {
    // Java7
    KeyValueGroupedDataset<String, Person> rst7 = ds.groupByKey(new MapFunction<Person, String>() {
      @Override
      public String call(Person p) throws Exception {
        return p.getJob();
      }
    }, Encoders.STRING());

    // Java8
    KeyValueGroupedDataset<String, Person> rst8 = ds.groupByKey((MapFunction<Person, String>) (Person p) -> p.getJob(),
            Encoders.STRING());

    rst7.agg(typed.sumLong(new MapFunction<Person, Long>() {
      @Override
      public Long call(Person person) throws Exception {
        return new Long(person.getAge());
      }
    })).show();

  }

  // 5.2.2.9절
  public static void runMapValueAndReduceGroups(SparkSession spark, Dataset<Person> ds) {
    // Java7
    KeyValueGroupedDataset<String, Person> rst7 = ds.groupByKey(new MapFunction<Person, String>() {
      @Override
      public String call(Person p) throws Exception {
        return p.getJob();
      }
    }, Encoders.STRING());

    // Java8
    KeyValueGroupedDataset<String, Person> rst8 = ds.groupByKey((MapFunction<Person, String>) (Person p) -> p.getJob(),
            Encoders.STRING());

    KeyValueGroupedDataset<String, String> ds1 = rst7.mapValues(new MapFunction<Person, String>() {
      @Override
      public String call(Person p) throws Exception {
        return p.getName() + "(" + p.getAge() + ") ";
      }
    }, Encoders.STRING());

    ds1.reduceGroups(new ReduceFunction<String>() {

      @Override
      public String call(String v1, String v2) throws Exception {
        return v1 + v1;
      }
    }).show(false);

  }

  public static void main(String[] args) {

    SparkSession spark = SparkSession
            .builder()
            .appName("DatasetSample")
            .master("local[*]")
            .getOrCreate();

    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

    Person row1 = new Person("hayoon", 7, "student");
    Person row2 = new Person("sunwoo", 13, "student");
    Person row3 = new Person("hajoo", 5, "kindergartener");
    Person row4 = new Person("jinwoo", 13, "student");

    List<Person> data = Arrays.asList(row1, row2, row3, row4);
    Dataset<Person> ds = spark.createDataset(data, Encoders.bean(Person.class));

    //createDataSet(spark, new JavaSparkContext(spark.sparkContext()));
    //runSelectEx(spark, ds);
    //runAsEx(spark);
    //runDistinctEx(spark);
    //runDropDuplicatesEx(spark, ds);
    //runFilterEx(spark, ds);
    //runFlatMap(spark);
    //runGroupByKeyEx(spark, ds);
    //runAgg(spark, ds);
    //runMapValueAndReduceGroups(spark, ds);

    spark.stop();
  }
}

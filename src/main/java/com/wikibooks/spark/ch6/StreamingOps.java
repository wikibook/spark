package com.wikibooks.spark.ch6;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;

public class StreamingOps {

  public static void main(String[] args) throws Exception {

    String checkpointDir = "./checkPoints/StreamingOps/Java";
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StreamingOps");
    JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
    JavaSparkContext sc = ssc.sparkContext();

    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "c", "c"));
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("1,2,3,4,5"));
    JavaRDD<String> rdd3 = sc.parallelize(Arrays.asList("k1,r1", "k2,r2", "k3,r3"));
    JavaRDD<String> rdd4 = sc.parallelize(Arrays.asList("k1,s1", "k2,s2"));
    JavaRDD<Integer> rdd5 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

    Queue<JavaRDD<String>> q1 = new LinkedList<>(Arrays.asList(rdd1));
    Queue<JavaRDD<String>> q2 = new LinkedList<>(Arrays.asList(rdd2));
    Queue<JavaRDD<String>> q3 = new LinkedList<>(Arrays.asList(rdd3));
    Queue<JavaRDD<String>> q4 = new LinkedList<>(Arrays.asList(rdd4));
    Queue<JavaRDD<Integer>> q5 = new LinkedList<>(Arrays.asList(rdd5));

    JavaDStream<String> ds1 = ssc.queueStream(q1, true);
    JavaDStream<String> ds2 = ssc.queueStream(q2, true);

    // Java7
    JavaPairDStream<String, String> ds3 = ssc.queueStream(q3, true)
            .mapToPair(new PairFunction<String, String, String>() {
              @Override
              public Tuple2<String, String> call(String str) throws Exception {
                String[] arr = str.split(",");
                return new Tuple2<String, String>(arr[0], arr[1]);
              }
            });

    // Java8
    JavaPairDStream<String, String> ds3_2 = ssc.queueStream(q3, true)
            .mapToPair((String str) -> {
              String[] arr = str.split(",");
              return new Tuple2<String, String>(arr[0], arr[1]);
            });

    // Java7
    JavaPairDStream<String, String> ds4 = ssc.queueStream(q4, true)
            .mapToPair(new PairFunction<String, String, String>() {
              @Override
              public Tuple2<String, String> call(String str) throws Exception {
                String[] arr = str.split(",");
                return new Tuple2<String, String>(arr[0], arr[1]);
              }
            });

    // Java8
    JavaPairDStream<String, String> ds4_2 = ssc.queueStream(q4, true)
            .mapToPair((String str) -> {
              String[] arr = str.split(",");
              return new Tuple2<String, String>(arr[0], arr[1]);
            });

    JavaDStream<Integer> ds5 = ssc.queueStream(q5, true);

    // 6.3.1절
    ds1.print();

    // 6.3.2절(Java7)
    ds1.map(new Function<String, Tuple2<String, Long>>() {
      @Override
      public Tuple2<String, Long> call(String str) throws Exception {
        return new Tuple2<String, Long>(str, 1L);
      }
    }).print();

    // 6.3.2절(Java8)
    ds1.map((String str) -> new Tuple2<String, Long>(str, 1L)).print();

    // 6.3.3절(Java7)
    ds2.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String str) throws Exception {
        return Arrays.asList(str.split(",")).iterator();
      }
    }).print();

    // 6.3.3절(Java8)
    ds2.flatMap((String str) -> Arrays.asList(str.split(",")).iterator()).print();

    // 6.3.4절
    ds1.count().print();
    ds1.countByValue().print();

    // 6.3.5절(Java7)
    ds1.reduce(new Function2<String, String, String>() {
      @Override
      public String call(String v1, String v2) throws Exception {
        return v1 + "," + v2;
      }
    }).print();

    ds1.mapToPair(new PairFunction<String, String, Long>() {
      @Override
      public Tuple2<String, Long> call(String s) throws Exception {
        return new Tuple2<String, Long>(s, 1L);
      }
    }).reduceByKey(new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    }).print();

    // 6.3.5절(Java8)
    ds1.reduce((String v1, String v2) -> v1 + "," + v2).print();

    ds1.mapToPair((String s) -> new Tuple2<String, Long>(s, 1L))
            .reduceByKey((Long v1, Long v2) -> v1 + v2)
            .print();

    // 6.3.6절(Java7)
    ds1.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String v1) throws Exception {
        return !StringUtils.equals(v1, "c");
      }
    }).print();

    // 6.3.6절(Java8)
    ds1.filter((String v1) -> !StringUtils.equals(v1, "c")).print();

    // 6.3.7절
    ds1.union(ds2).print();

    // 6.3.8절
    ds3.join(ds4).print();

    // 6.4.1절
    JavaRDD<Integer> otherRDD = sc.parallelize(Arrays.asList(1, 2));
    ds5.transform(new Function<JavaRDD<Integer>, JavaRDD<Integer>>() {
      @Override
      public JavaRDD<Integer> call(JavaRDD<Integer> myRDD) throws Exception {
        return myRDD.subtract(otherRDD);
      }
    }).print();

    // 6.4.2절
    JavaRDD<String> t1 = sc.parallelize(Arrays.asList("a", "b", "c"));
    JavaRDD<String> t2 = sc.parallelize(Arrays.asList("b", "c"));
    JavaRDD<String> t3 = sc.parallelize(Arrays.asList("a", "a", "a"));
    Queue<JavaRDD<String>> q6 = new LinkedList<>(Arrays.asList(t1, t2, t3));
    JavaDStream<String> ds6 = ssc.queueStream(q6, true);

    ssc.checkpoint(checkpointDir);

    // Java7
    ds6.mapToPair(new PairFunction<String, String, Long>() {
      @Override
      public Tuple2<String, Long> call(String s) throws Exception {
        return new Tuple2<String, Long>(s, 1L);
      }
    }).updateStateByKey(new Function2<List<Long>, org.apache.spark.api.java.Optional<Long>, Optional<Long>>() {
      @Override
      public Optional<Long> call(List<Long> newValues, Optional<Long> currentValue) throws Exception {
        Long sum = 0L;
        for (long v : newValues) sum += v;
        return Optional.of(currentValue.orElse(0L) + sum);
      }
    }).print();

    // Java8
    ds6.mapToPair((String s) -> new Tuple2<String, Long>(s, 1L))
            .updateStateByKey((List<Long> newValues, Optional<Long> currentValue) -> {
              return Optional.of(currentValue.orElse(0L) + newValues.stream().reduce(0L, Long::sum));
            }).print();

    // 6.4.3절
    List<JavaRDD<Long>> rdds = new ArrayList<>();
    for (long i = 1L; i <= 100L; i++) {
      rdds.add(sc.parallelize(Arrays.asList(i)));
    }

    JavaDStream<Long> ds7 = ssc.queueStream(new LinkedList<>(rdds));

    // 6.4.4절
    ds7.window(Durations.seconds(3), Durations.seconds(2)).print();

    // 6.4.5절
    ds7.countByWindow(Durations.seconds(3), Durations.seconds(2)).print();

    // 6.4.6절 (Java7)
    ds7.reduceByWindow(new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    }, Durations.seconds(3), Durations.seconds(2)).print();

    // 6.4.6절 (Java8)
    ds7.reduceByWindow((Long v1, Long v2) -> v1 + v2, Durations.seconds(3), Durations.seconds(2)).print();

    // 6.4.7절(Java7)
    ds7.mapToPair(new PairFunction<Long, Long, Long>() {
      @Override
      public Tuple2<Long, Long> call(Long v) throws Exception {
        return new Tuple2<Long, Long>(v % 2, 1L);
      }
    }).reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    }, Durations.seconds(4), Durations.seconds(2)).print();

    // 6.4.7절(Java8)
    ds7.mapToPair((Long v) -> new Tuple2<Long, Long>(v % 2, 1L))
            .reduceByKeyAndWindow((Long v1, Long v2) -> v1 + v2,
                    Durations.seconds(4), Durations.seconds(2)).print();

    // 역리듀스 함수 사용
    ds7.mapToPair(new PairFunction<Long, String, Long>() {
      @Override
      public Tuple2<String, Long> call(Long v) throws Exception {
        return new Tuple2<String, Long>("sum", v);
      }
    }).reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    }, new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 - v2;
      }
    }, Durations.seconds(3), Durations.seconds(2)).print();

    // 6.4.8절
    ds7.countByValueAndWindow(Durations.seconds(3), Durations.seconds(2)).print();

    // 6.5.1절
    ds6.dstream().saveAsTextFiles("/Users/beginspark/Temp/test", "dir");

    // 6.5.2절
    //runForeach(ssc)

    // 6.6절
    // CheckPointSample 참고

    ssc.start();
    ssc.awaitTermination();
  }
}
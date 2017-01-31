package com.wikibooks.spark.ch2;

import java.util.*;
import java.util.function.IntFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class RDDOpSample {

  public static void doCollect(JavaSparkContext sc) {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    List<Integer> result = rdd.collect();
    for (Integer i : result)
      System.out.println(i);
  }

  public static void doCount(JavaSparkContext sc) {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    long result = rdd.count();
    System.out.println(result);
  }

  public static void doMap(JavaSparkContext sc) {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

    // Java7
    JavaRDD<Integer> rdd2 = rdd1.map(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer v1) throws Exception {
        return v1 + 1;
      }
    });

    // Java8 Lambda
    JavaRDD<Integer> rdd3 = rdd1.map((Integer v1) -> v1 + 1);

    System.out.println(StringUtils.join(rdd2.collect(), ", "));
  }

  public static void doFlatMap(JavaSparkContext sc) {
    List<String> data = new ArrayList();
    data.add("apple,orange");
    data.add("grape,apple,mango");
    data.add("blueberry,tomato,orange");

    JavaRDD<String> rdd1 = sc.parallelize(data);

    JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String t) throws Exception {
        return Arrays.asList(t.split(",")).iterator();
      }
    });

    // Java8 Lambda
    JavaRDD<String> rdd3 = rdd1.flatMap((String t) -> Arrays.asList(t.split(",")).iterator());

    System.out.println(rdd2.collect());
  }

  public static void doMapPartitions(JavaSparkContext sc) {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
    JavaRDD<Integer> rdd2 = rdd1.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
      public Iterator<Integer> call(Iterator<Integer> numbers) throws Exception {
        System.out.println("DB연결 !!!");
        List<Integer> result = new ArrayList<>();
        while (numbers.hasNext()) {
          result.add(numbers.next());
        }
        return result.iterator();
      }

      ;
    });

    // Java8 Lambda
    JavaRDD<Integer> rdd3 = rdd1.mapPartitions((Iterator<Integer> numbers) -> {
      System.out.println("DB연결 !!!");
      List<Integer> result = new ArrayList<>();
      numbers.forEachRemaining(result::add);
      return result.iterator();
    });

    System.out.println(rdd3.collect());
  }

  public static void doMapPartitionsWithIndex(JavaSparkContext sc) {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);

    JavaRDD<Integer> rdd2 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
      @Override
      public Iterator<Integer> call(Integer idx, Iterator<Integer> numbers) throws Exception {
        List<Integer> result = new ArrayList<>();
        if (idx == 1) {
          while (numbers.hasNext()) {
            result.add(numbers.next());
          }
        }
        return result.iterator();
      }
    }, true);

    // Java8 Lambda
    JavaRDD<Integer> rdd3 = rdd2.mapPartitionsWithIndex((Integer idx, Iterator<Integer> numbers) -> {
      List<Integer> result = new ArrayList<>();
      if (idx == 1)
        numbers.forEachRemaining(result::add);
      return result.iterator();
    }, true);

    System.out.println(rdd2.collect());
    System.out.println(rdd3.collect());
  }

  public static void doMapValues(JavaSparkContext sc) {

    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));

    JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String t) throws Exception {
        return new Tuple2(t, 1);
      }
    });

    JavaPairRDD<String, Integer> rdd3 = rdd2.mapValues(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer v1) throws Exception {
        return v1 + 1;
      }
    });

    // Java8 Lambda
    JavaPairRDD<String, Integer> rdd4 = rdd1.mapToPair((String t) -> new Tuple2<String, Integer>(t, 1)).mapValues((Integer v1) -> v1 + 1);

    System.out.println(rdd3.collect());
  }

  public static void doFlatMapValues(JavaSparkContext sc) {

    List<Tuple2<Integer, String>> data = Arrays.asList(new Tuple2(1, "a,b"), new Tuple2(2, "a,c"), new Tuple2(1, "d,e"));

    JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(data);

    // Java7
    JavaPairRDD<Integer, String> rdd2 = rdd1.flatMapValues(new Function<String, Iterable<String>>() {
      @Override
      public Iterable<String> call(String v1) throws Exception {
        return Arrays.asList(v1.split(","));
      }
    });

    // Java8 Lambda
    JavaPairRDD<Integer, String> rdd3 = rdd1.flatMapValues((String v1) -> Arrays.asList(v1.split(",")));

    System.out.println(rdd2.collect());
  }

  public static void doZip(JavaSparkContext sc) {
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
    JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3));
    JavaPairRDD<String, Integer> result = rdd1.zip(rdd2);
    System.out.println(result.collect());
  }

  public static void doZipPartitions(JavaSparkContext sc) {
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"), 3);
    JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3), 3);

    // Java7
    JavaRDD<String> rdd3 = rdd1.zipPartitions(rdd2, new FlatMapFunction2<Iterator<String>, Iterator<Integer>, String>() {
      @Override
      public Iterator<String> call(Iterator<String> t1, Iterator<Integer> t2) throws Exception {
        List<String> list = new ArrayList<>();
        while (t1.hasNext()) {
          while (t2.hasNext()) {
            list.add(t1.next() + t2.next());
          }
        }
        return list.iterator();
      }
    });

    // Java8 Lambda
    JavaRDD<String> rdd4 = rdd1.zipPartitions(rdd2, (Iterator<String> t1, Iterator<Integer> t2) -> {
      List<String> list = new ArrayList<>();
      t1.forEachRemaining((String s) -> {
        t2.forEachRemaining((Integer i) -> list.add(s + i));
      });
      return list.iterator();
    });

    System.out.println(rdd3.collect());
  }

  public static void doGroupBy(JavaSparkContext sc) {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

    // Java7
    JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupBy(new Function<Integer, String>() {
      @Override
      public String call(Integer v1) throws Exception {
        return (v1 % 2 == 0) ? "even" : "odd";
      }
    });

    // Java8 Lambda
    JavaPairRDD<String, Iterable<Integer>> rdd3 = rdd1.groupBy((Integer v1) -> (v1 % 2 == 0) ? "even" : "odd");

    System.out.println(rdd2.collect());
  }

  public static void doGroupByKey(JavaSparkContext sc) {
    List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("c", 1), new Tuple2("b", 1), new Tuple2("c", 1));
    JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data);
    JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupByKey();
    System.out.println(rdd2.collect());
  }

  public static void doCogroup(JavaSparkContext sc) {
    List<Tuple2<String, String>> data1 = Arrays.asList(new Tuple2("k1", "v1"), new Tuple2("k2", "v2"), new Tuple2("k1", "v3"));
    List<Tuple2<String, String>> data2 = Arrays.asList(new Tuple2("k1", "v4"));

    JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(data1);
    JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(data2);

    JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> result = rdd1.<String>cogroup(rdd2);

    System.out.println(result.collect());
  }

  public static void doDistinct(JavaSparkContext sc) {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 1, 2, 3, 1, 2, 3));
    JavaRDD<Integer> result = rdd.distinct();
    System.out.println(result.collect());
  }

  public static void doCartesian(JavaSparkContext sc) {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b", "c"));
    JavaPairRDD<Integer, String> result = rdd1.cartesian(rdd2);
    System.out.println(result.collect());
  }

  public static void doSubtract(JavaSparkContext sc) {
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("d", "e"));
    JavaRDD<String> result = rdd1.subtract(rdd2);
    System.out.println(result.collect());
  }

  public static void doUnion(JavaSparkContext sc) {
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("d", "e", "f"));
    JavaRDD<String> result = rdd1.union(rdd2);
    System.out.println(result.collect());
  }

  public static void doIntersection(JavaSparkContext sc) {
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "a", "b", "c"));
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "a", "c", "c"));
    JavaRDD<String> result = rdd1.intersection(rdd2);
    System.out.println(result.collect());
  }

  public static void doJoin(JavaSparkContext sc) {
    List<Tuple2<String, Integer>> data1 = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("c", 1), new Tuple2("d", 1), new Tuple2("e", 1));
    List<Tuple2<String, Integer>> data2 = Arrays.asList(new Tuple2("b", 2), new Tuple2("c", 2));

    JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
    JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);

    JavaPairRDD<String, Tuple2<Integer, Integer>> result = rdd1.<Integer>join(rdd2);
    System.out.println(result.collect());
  }

  public static void doLeftOuterJoin(JavaSparkContext sc) {
    List<Tuple2<String, Integer>> data1 = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", "1"), new Tuple2("c", "1"));
    List<Tuple2<String, Integer>> data2 = Arrays.asList(new Tuple2("b", 2), new Tuple2("c", "2"));

    JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
    JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);

    JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> result1 = rdd1.<Integer>leftOuterJoin(rdd2);
    JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> result2 = rdd1.<Integer>rightOuterJoin(rdd2);
    System.out.println("Left: " + result1.collect());
    System.out.println("Right: " + result2.collect());
  }

  public static void doSubtractByKey(JavaSparkContext sc) {
    List<Tuple2<String, Integer>> data1 = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1));
    List<Tuple2<String, Integer>> data2 = Arrays.asList(new Tuple2("b", 2));

    JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
    JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);

    JavaPairRDD<String, Integer> result = rdd1.subtractByKey(rdd2);
    System.out.println(result.collect());
  }

  public static void doReduceByKey(JavaSparkContext sc) {
    List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("b", 1));

    JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);

    // Java7
    JavaPairRDD<String, Integer> result = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });

    // Java8 Lambda
    JavaPairRDD<String, Integer> result2 = rdd.reduceByKey((Integer v1, Integer v2) -> v1 + v2);
    System.out.println(result.collect());
  }

  public static void doFoldByKey(JavaSparkContext sc) {
    List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("b", 1));

    JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);

    // Java7
    JavaPairRDD<String, Integer> result = rdd.foldByKey(0, new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });

    // Java8 Lambda
    JavaPairRDD<String, Integer> result2 = rdd.foldByKey(0, (Integer v1, Integer v2) -> v1 + v2);
    System.out.println(result.collect());
  }

  public static void doCombineByKey(JavaSparkContext sc) {

    List<Tuple2<String, Long>> data = Arrays.asList(new Tuple2("Math", 100L), new Tuple2("Eng", 80L), new Tuple2("Math", 50L), new Tuple2("Eng", 70L), new Tuple2("Eng", 90L));

    JavaPairRDD<String, Long> rdd = sc.parallelizePairs(data);

    // Java7
    Function<Long, Record> createCombiner = new Function<Long, Record>() {
      @Override
      public Record call(Long v) throws Exception {
        return new Record(v);
      }
    };

    Function2<Record, Long, Record> mergeValue = new Function2<Record, Long, Record>() {
      @Override
      public Record call(Record record, Long v) throws Exception {
        return record.add(v);
      }
    };

    Function2<Record, Record, Record> mergeCombiners = new Function2<Record, Record, Record>() {
      @Override
      public Record call(Record r1, Record r2) throws Exception {
        return r1.add(r2);
      }
    };

    JavaPairRDD<String, Record> result = rdd.combineByKey(createCombiner, mergeValue, mergeCombiners);

    // Java8
    JavaPairRDD<String, Record> result2 = rdd.combineByKey((Long v) -> new Record(v), (Record record, Long v) -> record.add(v), (Record r1, Record r2) -> r1.add(r2));

    System.out.println(result.collect());
  }

  public static void doAggregateByKey(JavaSparkContext sc) {

    List<Tuple2<String, Long>> data = Arrays.asList(new Tuple2("Math", 100L), new Tuple2("Eng", 80L), new Tuple2("Math", 50L), new Tuple2("Eng", 70L), new Tuple2("Eng", 90L));

    JavaPairRDD<String, Long> rdd = sc.parallelizePairs(data);

    // Java7
    Record zero = new Record(0, 0);

    Function2<Record, Long, Record> mergeValue = new Function2<Record, Long, Record>() {
      @Override
      public Record call(Record record, Long v) throws Exception {
        return record.add(v);
      }
    };

    Function2<Record, Record, Record> mergeCombiners = new Function2<Record, Record, Record>() {
      @Override
      public Record call(Record r1, Record r2) throws Exception {
        return r1.add(r2);
      }
    };

    JavaPairRDD<String, Record> result = rdd.aggregateByKey(zero, mergeValue, mergeCombiners);

    // Java8
    JavaPairRDD<String, Record> result2 = rdd.aggregateByKey(zero, (Record record, Long v) -> record.add(v), (Record r1, Record r2) -> r1.add(r2));

    System.out.println(result.collect());
  }

  public static void doPipe(JavaSparkContext sc) {
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("1,2,3", "4,5,6", "7,8,9"));
    JavaRDD<String> result = rdd.pipe("cut -f 1,3 -d ,");
    System.out.println(result.collect());
  }

  public static void doCoalesceAndRepartition(JavaSparkContext sc) {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0), 10);
    JavaRDD<Integer> rdd2 = rdd1.coalesce(5);
    JavaRDD<Integer> rdd3 = rdd2.coalesce(10);
    System.out.println("partition size:" + rdd1.getNumPartitions());
    System.out.println("partition size:" + rdd2.getNumPartitions());
    System.out.println("partition size:" + rdd3.getNumPartitions());
  }

  public static void doRepartitionAndSortWithinPartitions(JavaSparkContext sc) {
    List<Integer> data = fillToNRandom(10);
    JavaPairRDD<Integer, String> rdd1 = sc.parallelize(data).mapToPair((Integer v) -> new Tuple2(v, "-"));
    JavaPairRDD<Integer, String> rdd2 = rdd1.repartitionAndSortWithinPartitions(new HashPartitioner(3));

    rdd2.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, String>>>() {
      @Override
      public void call(Iterator<Tuple2<Integer, String>> it) throws Exception {
        System.out.println("==========");
        while (it.hasNext()) {
          System.out.println(it.next());
        }
      }
    });
  }

  public static void doPartitionBy(JavaSparkContext sc) {
    List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("apple", 1), new Tuple2("mouse", 1), new Tuple2("monitor", 1));
    JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data, 5);
    JavaPairRDD<String, Integer> rdd2 = rdd1.partitionBy(new HashPartitioner(3));
    System.out.println("rdd1:" + rdd1.getNumPartitions() + ", rdd2:" + rdd2.getNumPartitions());
  }

  public static void doFilter(JavaSparkContext sc) {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaRDD<Integer> result = rdd.filter(new Function<Integer, Boolean>() {
      @Override
      public Boolean call(Integer v1) throws Exception {
        return v1 > 2;
      }
    });
    System.out.println(result.collect());
  }

  public static void doSortByKey(JavaSparkContext sc) {
    List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("q", 1), new Tuple2("z", 1), new Tuple2("a", 1));
    JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);
    JavaPairRDD<String, Integer> result = rdd.sortByKey();
    System.out.println(result.collect());
  }

  public static void doKeysAndValues(JavaSparkContext sc) {
    List<Tuple2<String, String>> data = Arrays.asList(new Tuple2("k1", "v1"), new Tuple2("k2", "v2"), new Tuple2("k3", "v3"));
    JavaPairRDD<String, String> rdd = sc.parallelizePairs(data);
    System.out.println(rdd.keys().collect());
    System.out.println(rdd.values().collect());
  }

  public static void doSample(JavaSparkContext sc) {
    List<Integer> data = fillToN(100);
    JavaRDD<Integer> rdd = sc.parallelize(data);
    JavaRDD<Integer> result1 = rdd.sample(false, 0.5);
    JavaRDD<Integer> result2 = rdd.sample(true, 1.5);
    System.out.println(result1.take(5));
    System.out.println(result2.take(5));
  }

  public static void doFirst(JavaSparkContext sc) {
    List<Integer> data = Arrays.asList(5, 4, 1);
    JavaRDD<Integer> rdd = sc.parallelize(data);
    int result = rdd.first();
    System.out.println(result);
  }

  public static void doTake(JavaSparkContext sc) {
    List<Integer> data = fillToN(100);
    JavaRDD<Integer> rdd = sc.parallelize(data);
    List<Integer> result = rdd.take(5);
    System.out.println(result);
  }

  public static void doTakeSample(JavaSparkContext sc) {
    List<Integer> data = fillToN(100);
    JavaRDD<Integer> rdd = sc.parallelize(data);
    List<Integer> result = rdd.takeSample(false, 20);
    System.out.println(result.size());
  }

  public static void doCountByValue(JavaSparkContext sc) {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 3));
    Map<Integer, Long> result = rdd.countByValue();
    System.out.println(result);
  }

  public static void doReduce(JavaSparkContext sc) {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD<Integer> rdd = sc.parallelize(data, 3);
    // Java7
    int result = rdd.reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });
    // Java8
    int result2 = rdd.reduce((Integer v1, Integer v2) -> v1 + v2);
    System.out.println(result);
  }

  public static void doFold(JavaSparkContext sc) {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD<Integer> rdd = sc.parallelize(data, 3);
    // Java7
    int result = rdd.fold(0, new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });
    // Java8
    int result2 = rdd.fold(0, (Integer v1, Integer v2) -> v1 + v2);
    System.out.println(result);
  }

  public static void doAggregate(JavaSparkContext sc) {

    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(100, 80, 75, 90, 95), 3);

    Record zeroValue = new Record(0, 0);

    // Java7
    Function2<Record, Integer, Record> seqOp = new Function2<Record, Integer, Record>() {
      @Override
      public Record call(Record r, Integer v) throws Exception {
        return r.add(v);
      }
    };

    Function2<Record, Record, Record> combOp = new Function2<Record, Record, Record>() {
      @Override
      public Record call(Record r1, Record r2) throws Exception {
        return r1.add(r2);
      }
    };

    Record result = rdd.aggregate(zeroValue, seqOp, combOp);

    // Java8
    Function2<Record, Integer, Record> seqOp2 = (Record r, Integer v) -> r.add(v);
    Function2<Record, Record, Record> combOp2 = (Record r1, Record r2) -> r1.add(r2);
    Record result2 = rdd.aggregate(zeroValue, seqOp2, combOp2);

    System.out.println(result);
  }

  public static void doSum(JavaSparkContext sc) {
    List<Double> data = Arrays.asList(1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d);
    JavaDoubleRDD rdd = sc.parallelizeDoubles(data);
    double result = rdd.sum();
    System.out.println(result);
  }

  public static void doForeach(JavaSparkContext sc) {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD<Integer> rdd = sc.parallelize(data);
    // Java7
    rdd.foreach(new VoidFunction<Integer>() {
      @Override
      public void call(Integer t) throws Exception {
        System.out.println("Value Side Effect: " + t);
      }
    });
    // Java8
    rdd.foreach((Integer t) -> System.out.println("Value Side Effect: " + t));
  }

  public static void doForeachPartition(JavaSparkContext sc) {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD<Integer> rdd = sc.parallelize(data, 3);
    // Java7
    rdd.foreachPartition(new VoidFunction<Iterator<Integer>>() {
      @Override
      public void call(Iterator<Integer> it) throws Exception {
        System.out.println("Partition Side Effect!!");
        while (it.hasNext())
          System.out.println("Value Side Effect: " + it.next());
      }
    });
    // Java8
    rdd.foreachPartition((Iterator<Integer> it) -> {
      System.out.println("Partition Side Effect!!");
      it.forEachRemaining(v -> System.out.println("Value Side Effect:" + v));
    });
  }

  public static void doDebugString(JavaSparkContext sc) {
    JavaRDD<Integer> rdd1 = sc.parallelize(fillToN(100), 10);
    JavaRDD<Integer> rdd2 = rdd1.map((Integer v1) -> v1 * 2);
    JavaRDD<Integer> rdd3 = rdd2.map((Integer v1) -> v1 * 2);
    JavaRDD<Integer> rdd4 = rdd3.coalesce(2);
    System.out.println(rdd4.toDebugString());
  }

  public static void doCache(JavaSparkContext sc) {
    JavaRDD<Integer> rdd = sc.parallelize(fillToN(100), 10);
    rdd.cache();
    rdd.persist(StorageLevel.MEMORY_ONLY());
  }

  public static void doGetPartitions(JavaSparkContext sc) {
    JavaRDD<Integer> rdd = sc.parallelize(fillToN(1000), 10);
    System.out.println(rdd.partitions().size());
    System.out.println(rdd.getNumPartitions());
  }

  public static void saveAndLoadTextFile(JavaSparkContext sc) {
    JavaRDD<Integer> rdd = sc.parallelize(fillToN(1000), 3);
    Class codec = org.apache.hadoop.io.compress.GzipCodec.class;
    // save
    rdd.saveAsTextFile("<path_to_save>/sub1");
    // save(gzip)
    rdd.saveAsTextFile("<path_to_save>/sub2", codec);
    // load
    JavaRDD<String> rdd2 = sc.textFile("<path_to_save>/sub1");
    System.out.println(rdd2.take(10));
  }

  public static void saveAndLoadObjectFile(JavaSparkContext sc) {
    JavaRDD<Integer> rdd = sc.parallelize(fillToN(1000), 3);
    // save
    rdd.saveAsObjectFile("<path_to_save>/sub_path");
    // load
    JavaRDD<Integer> rdd2 = sc.objectFile("<path_to_save>/sub_path");
    System.out.println(rdd2.take(10));
  }

  public static void saveAndLoadSequenceFile(JavaSparkContext sc) {

    // 아래 경로는 실제 저장 경로로 변경하여 테스트
    String path = "data/sample/saveAsSeqFile/java";

    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "b", "c"));

    // Writable로 변환 - Java7
    JavaPairRDD<Text, LongWritable> rdd2 = rdd1.mapToPair(new PairFunction<String, Text, LongWritable>() {
      @Override
      public Tuple2<Text, LongWritable> call(String v) throws Exception {
        return new Tuple2<Text, LongWritable>(new Text(v), new LongWritable(1));
      }
    });

    // Writable로 변환 - Java8
    JavaPairRDD<Text, LongWritable> rdd2_1 = rdd1.mapToPair((String v) -> new Tuple2<Text, LongWritable>(new Text(v), new LongWritable(1)));

    // SequenceFile로 저장
    rdd2.saveAsNewAPIHadoopFile(path, Text.class, LongWritable.class, SequenceFileOutputFormat.class);

    // SequenceFile로 부터 RDD 생성
    JavaPairRDD<Text, LongWritable> rdd3 = sc.newAPIHadoopFile(path, SequenceFileInputFormat.class, Text.class, LongWritable.class, new Configuration());

    // Writable을 String으로 변환 - Java7
    JavaRDD<String> rdd4 = rdd3.map(new Function<Tuple2<Text, LongWritable>, String>() {
      @Override
      public String call(Tuple2<Text, LongWritable> v1) throws Exception {
        return v1._1().toString() + v1._2;
      }
    });

    // Writable을 String으로 변환 - Java8
    JavaRDD<String> rdd4_1 = rdd3.map((Tuple2<Text, LongWritable> v1) -> v1._1().toString());

    // 결과 출력
    System.out.println(rdd4.collect());
  }

  public static void testBroadcaset(JavaSparkContext sc) {
    Broadcast<Set<String>> bu = sc.broadcast(new HashSet<String>(Arrays.asList("u1", "u2")));
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("u1", "u3", "u3", "u4", "u5", "u6"), 3);

    // Java7
    JavaRDD<String> result = rdd.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String v1) throws Exception {
        return bu.value().contains(v1);
      }
    });

    // Java8
    JavaRDD<String> result2 = rdd.filter((String v1) -> bu.value().contains(v1));

    System.out.println(result.collect());
  }

  public static void main(String[] args) throws Exception {
    JavaSparkContext sc = getSparkContext();
    saveAndLoadSequenceFile(sc);
    sc.stop();
  }

  public static JavaSparkContext getSparkContext() {
    SparkConf conf = new SparkConf().setAppName("RDDOpSample").setMaster("local[*]");
    return new JavaSparkContext(conf);
  }

  public static ArrayList<Integer> fillToN(int n) {
    ArrayList<Integer> rst = new ArrayList<>();
    for (int i = 0; i < n; i++)
      rst.add(i);
    return rst;
  }

  public static List<Integer> fillToNRandom(int n) {
    ArrayList<Integer> rst = new ArrayList<>();
    Random random = new Random();
    return random.ints(n, 0, 100).boxed().collect(Collectors.toList());
  }
}
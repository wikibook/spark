package com.wikibooks.spark.ch2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

public class AccumulatorSample {

  // ex 2-141
  public static void runBuitInAcc(JavaSparkContext jsc) {
    LongAccumulator acc1 = jsc.sc().longAccumulator("invalidFormat");
    CollectionAccumulator acc2 = jsc.sc().collectionAccumulator("invalidFormat2");
    List<String> data = Arrays.asList("U1:Addr1", "U2:Addr2", "U3", "U4:Addr4", "U5;Addr5", "U6:Addr6", "U7::Addr7");
    jsc.parallelize(data, 3).foreach(new VoidFunction<String>() {
      @Override
      public void call(String v) throws Exception {
        if (v.split(":").length != 2) {
          acc1.add(1L);
          acc2.add(v);
        }
      }
    });
    System.out.println("잘못된 데이터 수:" + acc1.value());
    System.out.println("잘못된 데이터:" + acc2.value());
  }

  // ex 2-144
  public static void runCustomAcc(JavaSparkContext jsc) {
    RecordAccumulator acc = new RecordAccumulator();
    jsc.sc().register(acc, "invalidFormat");
    List<String> data = Arrays.asList("U1:Addr1", "U2:Addr2", "U3", "U4:Addr4", "U5;Addr5", "U6:Addr6", "U7::Addr7");
    jsc.parallelize(data, 3).foreach(new VoidFunction<String>() {
      @Override
      public void call(String v) throws Exception {
        if (v.split(":").length != 2) {
          acc.add(new Record(1L));
        }
      }
    });
    System.out.println("잘못된 데이터 수:" + acc.value());
  }

  public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf().setAppName("AccumulatorSample").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    runCustomAcc(sc);
    sc.stop();
  }
}

class RecordAccumulator extends AccumulatorV2<Record, Long> {

  private static final long serialVersionUID = 1L;

  private Record _record = new Record(0L);

  @Override
  public boolean isZero() {
    return _record.amount == 0L && _record.number == 1L;
  }

  @Override
  public AccumulatorV2<Record, Long> copy() {
    RecordAccumulator newAcc = new RecordAccumulator();
    newAcc._record = new Record(_record.amount, _record.number);
    return newAcc;
  }

  @Override
  public void reset() {
    _record.amount = 0L;
    _record.number = 1L;
  }

  @Override
  public void add(Record other) {
    _record.add(other);
  }

  @Override
  public void merge(AccumulatorV2<Record, Long> otherAcc) {
    try {
      Record other = ((RecordAccumulator) otherAcc)._record;
      _record.add(other);
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }

  @Override
  public Long value() {
    return _record.amount;
  }
}

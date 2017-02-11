package com.wikibooks.spark.ch2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

// 예제 2-3
class Add implements Function<Integer, Integer> {
	@Override
	public Integer call(Integer v1) throws Exception {
		return v1 + 1;
	}
}

public class PassingFunctionSample {

	public void runMapSample(JavaSparkContext sc) {

		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

		JavaRDD<Integer> rdd2 = rdd1.map(new Add());

		System.out.println(rdd2.collect());

		sc.stop();
	}

	public static void main(String[] args) throws Exception {

		JavaSparkContext sc = getSparkContext();

		PassingFunctionSample sample = new PassingFunctionSample();

		sample.runMapSample(sc);

		sc.stop();
	}

	public static JavaSparkContext getSparkContext() {
		SparkConf conf = new SparkConf().setAppName("PassingFunctionSample").setMaster("local[*]");
		return new JavaSparkContext(conf);
	}
}

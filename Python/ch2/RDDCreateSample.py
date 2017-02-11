from pyspark import SparkContext, SparkConf
import os


class RDDCreateSample:
    def run(self, sc):

        # 2.1.3절 예제 2-6
        rdd1 = sc.parallelize(["a", "b", "c", "d", "e"])

        # 2.1.3절 예제 2-8
        rdd2 = sc.textFile("<spark_home_dir>/README.md")

        print(rdd1.collect())
        print(rdd2.collect())


if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(master="local[*]", appName="RDDCreateTest", conf=conf)

    obj = RDDCreateSample()
    obj.run(sc)

    sc.stop()

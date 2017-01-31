from pyspark import SparkContext, SparkConf
import os


class RDDCreateSample:
    def run(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c", "d", "e"])
        # <project_home>은 예제 프로젝트의 경로
        rdd2 = sc.textFile("file://<project_home>/source/data/sample.txt")

        print(rdd1.collect())
        print(rdd2.collect())


if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(master="local[*]", appName="RDDCreateTest", conf=conf)

    obj = RDDCreateSample()
    obj.run(sc)

    sc.stop()

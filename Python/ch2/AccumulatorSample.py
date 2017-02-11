from pyspark import SparkContext, SparkConf, AccumulatorParam
from record import Record
from builtins import isinstance

class AccumulatorSample():

    # ex 2-142
    def runBuitInAcc(self, sc):
        acc1 = sc.accumulator(0)
        data = ["U1:Addr1", "U2:Addr2", "U3", "U4:Addr4", "U5;Addr5", "U6:Addr6", "U7::Addr7"]
        rdd = sc.parallelize(data)
        rdd.foreach(lambda v: accumulate(v, acc1))
        print(acc1.value)

    # ex 2-145
    def runCustomAcc(self, sc):
        acc = sc.accumulator(Record(0), RecordAccumulatorParam())
        data = ["U1:Addr1", "U2:Addr2", "U3", "U4:Addr4", "U5;Addr5", "U6:Addr6", "U7::Addr7"]
        rdd = sc.parallelize(data)
        rdd.foreach(lambda v: accumulate(v, acc))
        print(acc.value.amount)


def accumulate(v, acc):
    if (len(v.split(":")) != 2):
        acc.add(1)


class RecordAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return Record(0)

    def addInPlace(self, v1, v2):
        if (isinstance(v2, Record)):
            return v1 + v2
        else:
            return v1.addAmt(v2)


if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(master="local[*]", appName="AccumulatorSample", conf=conf)

    obj = AccumulatorSample()
    obj.runCustomAcc(sc)
    sc.stop()

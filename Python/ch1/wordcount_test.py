import unittest
from pyspark import SparkContext, SparkConf
from wordcount import WordCount

class WordCountTest(unittest.TestCase):

    def testWordCount(self):
        wc = WordCount()
        sc = wc.getSparkContext("WordCountTest", "local[*]")
        input = ["Apache Spark is a fast and general engine for large-scale data processing.", "Spark runs on both Windows and UNIX-like systems"]
        inputRDD = sc.parallelize(input)
        resultRDD = wc.process(inputRDD)
        resultMap = resultRDD.collectAsMap()
        
        self.assertEqual(resultMap['Spark'], 2)
        self.assertEqual(resultMap['UNIX-like'], 1)
        self.assertEqual(resultMap['runs'], 1)
        
        print(resultMap)
        
        sc.stop()
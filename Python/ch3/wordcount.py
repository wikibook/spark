from pyspark import SparkContext, SparkConf
import sys

# 3.2.1.5.3절
class WordCount:
    
    def run(self, inputPath, outputPath):
        conf = SparkConf()        
        sc = SparkContext(conf=conf)
        sc.textFile(inputPath).flatMap(lambda s : s.split(" ")).map(lambda s: (s, 1)).reduceByKey(lambda v1, v2:v1 + v2).saveAsTextFile(outputPath)
        sc.stop()
        
if __name__ == "__main__":
    
    app = WordCount()
    
    if len(sys.argv) != 3:
        print("Usage: $SPARK_HOME/bin/spark-submit --master <master> --<option> <option_value> <python_file_path> <input_path> <output_path>")
    else:
        app.run(sys.argv[1], sys.argv[2])

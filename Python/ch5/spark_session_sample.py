from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ex5-3, ex5-14
spark = SparkSession\
          .builder\
          .appName("Sample")\
          .master("local[*]")\
          .getOrCreate()

# ex5-6
source = "file:///Users/beginspark/Apps/spark/README.md"
df = spark.read.text(source)

# ex5-9
wordDF = df.select(explode(split(col("value"), " ")).alias("word"))
result = wordDF.groupBy("word").count()
result.show()

spark.stop()
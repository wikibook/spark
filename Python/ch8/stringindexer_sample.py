from pyspark.ml.feature import IndexToString
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("stringindexer_sample") \
    .master("local[*]") \
    .getOrCreate()

df1 = spark.createDataFrame([
    (0, "red"),
    (1, "blue"),
    (2, "green"),
    (3, "yellow")]).toDF("id", "color")

strignIndexer = StringIndexer(inputCol="color", outputCol="colorIndex").fit(df1)

df2 = strignIndexer.transform(df1)

df2.show()

indexToString = IndexToString(inputCol="colorIndex", outputCol="originalColor")

df3 = indexToString.transform(df2)
df3.show()

spark.stop

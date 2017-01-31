from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import HashingTF
from pyspark.ml.feature import IDF
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("tf_idf_sample") \
    .master("local[*]") \
    .getOrCreate()

df1 = spark.createDataFrame([
    (0, "a a a b b c"),
    (0, "a b c"),
    (1, "a c a a d")]).toDF("label", "sentence")

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

# 각 문장을 단어로 분리
df2 = tokenizer.transform(df1)

hashingTF = HashingTF(inputCol="words", outputCol="TF-Features", numFeatures=20)
df3 = hashingTF.transform(df2)

df3.cache()

idf = IDF(inputCol="TF-Features", outputCol="Final-Features")
idfModel = idf.fit(df3)

rescaledData = idfModel.transform(df3)
rescaledData.select("words", "TF-Features", "Final-Features").show()

spark.stop

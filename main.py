from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("This example about to Dilisim Quiz") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

data = spark.read.text("README.md")

wordCounts = data.select(explode(split(data.value, "\s+")).alias("word")).groupBy("word").count()

wordCounts.createOrReplaceTempView("TextTable")
sqlDF = spark.sql("SELECT * FROM TextTable where word in ('Spark', 'spark')")
print("Sadece eşsiz bir şekilde kelimeyi içeren kayıtların sonucu")
sqlDF.show()
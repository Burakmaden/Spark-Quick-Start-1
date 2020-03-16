from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("This example about to Dilisim Quiz") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

data = spark.read.text("README.md")

sparkCount = data.filter(data.value.contains("Spark")).count()
sparkCount2 = data.filter(data.value.contains("spark")).count()

print("UpperCase start 'Spark' count:", sparkCount)
print("LowerCase start 'spark' count:", sparkCount2)

print("Total any case 'spark' count:", sparkCount + sparkCount2)
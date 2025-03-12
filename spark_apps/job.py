from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestJob").getOrCreate()

data = [("Alice", 29), ("Bob", 35), ("Charlie", 40)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show()

spark.stop()

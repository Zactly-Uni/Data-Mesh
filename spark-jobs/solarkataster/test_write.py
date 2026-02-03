from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([(1,"a"),(2,"b")], ["id","val"])
df.coalesce(1).write.mode("overwrite").option("header","true").csv("file:///tmp/test_out")

print("WROTE /tmp/test_out")
spark.stop()

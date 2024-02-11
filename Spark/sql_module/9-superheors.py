from pyspark.sql import SparkSession, functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType 

spark = SparkSession.builder.appName("PopularHeros").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True), 
    StructField("name", StringType(), True)
])

names = spark.read.option("sep", " ").schema(schema).csv("../csvs/Marvel_Names.txt")
lines = spark.read.text("../csvs/Marvel_Graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])    \
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(f" {mostPopularName[0]} is the most popular hero with {mostPopular[1]} co-actors.")


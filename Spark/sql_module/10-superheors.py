from pyspark.sql import SparkSession, functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType 

spark = SparkSession.builder.appName("ObscureHeros").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True), 
    StructField("name", StringType(), True)
])

names = spark.read.option("sep", " ").schema(schema).csv("../csvs/Marvel_Names.txt")
lines = spark.read.text("../csvs/Marvel_Graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])    \
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Filter connections with the minimum connection
mostPopularConnection = connections.sort(func.col("connections").asc()).first()
print(f"Least number of connections is {mostPopularConnection[1]} .")

herosWithMinConnection = connections.filter(func.col("connections") == mostPopularConnection[1])
herosWithMinConnectionNames = herosWithMinConnection.join(names, "id")
herosWithMinConnectionNames.select("name").show()





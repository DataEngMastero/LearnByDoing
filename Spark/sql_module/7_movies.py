from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

df = spark.read.option("sep", "\t" ).schema(schema).csv("../ml-100k/u.data")
df.printSchema()


ratingsByMovie = df.groupBy("movieID").count().orderBy(func.desc("count"))
ratingsByMovie.show(10)

spark.stop()
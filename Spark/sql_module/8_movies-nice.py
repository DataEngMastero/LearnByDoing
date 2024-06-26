from pyspark.sql import SparkSession, functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, LongType 
import codecs

def loadMoviesName():
    movieNames = {}

    with codecs.open("../ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
nameDict = spark.sparkContext.broadcast(loadMoviesName())

schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

df = spark.read.option("sep", "\t" ).schema(schema).csv("../ml-100k/u.data")
df.printSchema()


ratingsByMovie = df.groupBy("movieID").count()
ratingsByMovie.show(10)

def lookupName(movieID):
    return nameDict.value[movieID]

lookupNameUDF = func.udf(lookupName)

moviesWithName = ratingsByMovie.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))
sortedMovies = moviesWithName.orderBy(func.desc("count"))
sortedMovies.show(10, False)

spark.stop()
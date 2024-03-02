import sys
from pyspark.sql import SparkSession, functions as func 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

def computeCosineSimilarity(data):
    
    pairScores = data.withColumn("xx", func.col("rating1") * func.col("rating1")) \
                    .withColumn("yy", func.col("rating2") * func.col("rating2"))   \
                    .withColumn("xy", func.col("rating1") * func.col("rating2"))

    calculateSimilarity = pairScores.groupBy("movie1", "movie2")    \
                    .agg(   
                        func.sum(func.col("xy")).alias("numerator"),
                        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"),
                         func.count(func.col("xy")).alias("numPairs")
                    )
    
    result = calculateSimilarity.withColumn("score", 
                func.when(func.col("denominator") != 0, func.col("numerator")/ func.col("denominator")).otherwise(0)
            ).select("movie1", "movie2", "score", "numPairs")
    return result

def getMoviesName(movies, movieID):
    result = movies.filter(func.col("movieID") == movieID).select("movieTitle").collect()[0]
    return result[0] 


spark = SparkSession.builder.appName("MovieRecomm").master("local[*]").getOrCreate()

movieNameSchema = StructType([
    StructField("movieID", IntegerType(), True),
    StructField("movieTitle", StringType(), True)
])

movieSchema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True),
])


movieNames = spark.read.option("sep", "|").option("charset", "ISO-8859-1").schema(movieNameSchema).csv("../ml-100k/u.item")
movies = spark.read.option("sep", "\t").schema(movieSchema).csv("../ml-100k/u.data")

ratings = movies.select("userID", "movieID", "rating")

moviePairs = ratings.alias("ratings1").join(ratings.alias("ratings2"), (func.col("ratings1.userID") == func.col("ratings2.userID"))
                & (func.col("ratings1.movieID") < func.col("ratings2.movieID")))   \
            .select(func.col("ratings1.movieID").alias("movie1"), func.col("ratings2.movieID").alias("movie2"),
                    func.col("ratings1.rating").alias("rating1"), func.col("ratings2.rating").alias("rating2"))

moviePairSimilarities = computeCosineSimilarity(moviePairs).cache()


if(len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = sys.argv[1]

    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &
        (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold)
    )


    results = filteredResults.sort(func.col("score").desc()).take(10)
    
    print("Top 10 similar movies for  " + getMoviesName(movieNames, movieID))

    for result in results:
        similarMovieId = result.movie1
        if(similarMovieId == movieID):
            similarMovieId = result.movie2
        
        print(getMoviesName(movieNames, similarMovieId) + "\t score : " + str(result.score) + "\t strength : " + str(result.numPairs))

























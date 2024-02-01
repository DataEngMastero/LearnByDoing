from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("../csvs/Book.txt")

words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowercaseWords = words.select(func.lower(words.word).alias("word"))
wordCounts = lowercaseWords.groupBy("word").count()
wordCountsSorted = wordCounts.sort("count")

wordCountsSorted.show(wordCountsSorted.count())

spark.stop()
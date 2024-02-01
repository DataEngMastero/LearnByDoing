from pyspark import SparkContext, SparkConf
import collections
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

def normalizeWord(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


lines = sc.textFile("../csvs/Book.txt")
words = lines.flatMap(normalizeWord)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y : x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()


for result in results:
    count = str(result[0])
    word = str(result[1]).encode('ascii', 'ignore').decode('ascii')
    if word:
        print(word + ": \t\t" + count)

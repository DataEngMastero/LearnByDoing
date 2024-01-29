from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

lines = sc.textFile("csvs/Book.txt")
words = lines.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord, count)

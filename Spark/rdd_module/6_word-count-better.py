from pyspark import SparkContext, SparkConf
import collections
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

def normalizeWord(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


lines = sc.textFile("../csvs/Book.txt")
words = lines.flatMap(normalizeWord)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore').decode('ascii')
    if cleanWord:
        print(cleanWord, count)

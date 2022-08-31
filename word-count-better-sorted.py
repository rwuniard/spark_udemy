import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Users/rwuniard/projects/udemy_spark/book.txt")
words = input.flatMap(normalizeWords)

# You make each word to have a count of 1, then you can use the reduceByKey() function to add up the counts.
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# This code is to swap the count to be in the first column and the word to be in the second column.
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

# Print the results.
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)

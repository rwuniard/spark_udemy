from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Users/rwuniard/projects/udemy_spark/test.txt")
# This seems to generate an error. It seems there is an issue handling a list, compare to a dictionary in the countByValue() function.
words = input.map(lambda x: x.split())
resultsmap = words.collect()

for resultmap in resultsmap:
    print(resultmap)


flat_words = input.flatMap(lambda x: x.split())
results = flat_words.collect()

for result in results:
    print(result)
# This exercise is to find average number of friends by age.

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///Users/rwuniard/projects/udemy_spark/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

friendsByAge = people.select("age", "friends")
friendsByAge.groupBy("age").avg("friends").show()
friendsByAge.groupBy("age").avg("friends").orderBy("age").show()
# or you can use sort
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# To format it nicely. We are using the func and agg. 
# func is imported above.
# agg is to aggregate the that is grouped by age.
# then we call the func.round() to round the result to 2 decimal places.
# then we call func.avg() to get the average.
# The alias is to name the column with whatever we want to call it.
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("avg_friends")).show()

# This is to sort it based on age.
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("avg_friends")).sort("age").show()
spark.stop()


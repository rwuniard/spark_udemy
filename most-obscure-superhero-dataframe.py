from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///Users/rwuniard/projects/udemy_spark/Marvel+Names")

lines = spark.read.text("file:///Users/rwuniard/projects/udemy_spark/Marvel+Graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
# The first line below is to split the first id from the rest of the line. That's the superhero ID.
# The second line is to find how many values are in the line, then we substract it by 1 to get the number of connections.
# We subtract 1 because the first value is the superhero ID.
# THe third line is to sum the number of connections for each superhero.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

   
onlyOneConnection = connections.filter(func.col("connections") == 1)

# It is best practice to use the smaller dataframe to join with the larger one.
oneConnectionNames = onlyOneConnection.join(names, names.id == onlyOneConnection.id).select("name")
print ("The following heroes have only one connection: 1")
oneConnectionNames.show()
# for name in oneConnectionNames.collect():
#     print(name[0])

# Find the minimum number of connections.
# This will return the number of min connection number only.
minConnection = connections.agg(func.min("connections")).first()[0]
print(minConnection)

# We use the minimum number of connections to find the superheroes with that many connections.
minHeroConnection = connections.filter(func.col("connections") == minConnection)

# Find the name of these superheroes.
minHeroNames = minHeroConnection.join(names, names.id == minHeroConnection.id).select("name")
print ("These heroes have the minimum number of connections: " + str(minConnection))
minHeroNames.show()

spark.stop()



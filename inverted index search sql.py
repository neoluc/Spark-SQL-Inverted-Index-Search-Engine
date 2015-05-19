from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import argparse

# this illustrate basic idea in search engines

def search(input_path, search_string):
	sc = SparkContext("local", "Inverted Index")
	sqlc = SQLContext(sc)

	# build the inverted index table and push to db
	rdd = sc.wholeTextFiles(input_path)\
		.flatMap(lambda (name, content): map(lambda word: (word, name), content.split()))\
		.map(lambda (word, name): ((word, name), 1))\
		.reduceByKey(lambda count1, count2: count1 + count2)\
		.map(lambda ((word, name), count): (word, name, count))

	fields = [StructField("word", StringType(), False), StructField("name", StringType(), False), StructField("count", IntegerType(), False)]
	schema = sqlc.createDataFrame(rdd, StructType(fields))
	schema.registerTempTable("index_table")

	# query from db and compute the result
	search_condition = " OR ".join(map(lambda keyword: "word = '%s'" % keyword, search_string.split()))

	result_rdd = schema.select("*")\
		.filter(search_condition)\
		.map(lambda row: (row.name, row.count))\
		.reduceByKey(lambda count1, count2: count1 + count2)\
		.sortBy((lambda (name, count): count), False)\
		.map(lambda (name, count): name + " has number of hits: " + `count`)

	#print the result
	for line in result_rdd.collect():
		print line
		
	sc.stop()

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description = "Return search results.")
	parser.add_argument("-i", "--input", type = str, help = "input directory you want to search")
	parser.add_argument("-s", "--search", type = str, help = "search string")
	args = parser.parse_args()
	search(args.input, args.search)
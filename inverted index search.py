from pyspark import SparkContext
import argparse

# this illustrate basic idea in search engines

def search(input_path, search_string):
	sc = SparkContext("local", "Inverted Index")

	# build the inverted index
	inverted_index_rdd = sc.wholeTextFiles(input_path)\
		.flatMap(lambda (name, content): map(lambda word: (word, name), content.split()))\
		.map(lambda (word, name): ((word, name), 1))\
		.reduceByKey(lambda count1, count2: count1 + count2)\
		.map(lambda ((word, name), count): (word, (name, count)))\
		.groupByKey()
	search_keywords = set(search_string.split())

	# query the result from inverted index
	result_rdd = inverted_index_rdd.filter(lambda (word, name_count_list): word in search_keywords)\
		.flatMap(lambda (word, name_count_list): name_count_list)\
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
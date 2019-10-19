from pyspark import SparkContext
from pyspark.streaming import StreamingContext
print("ssssss")
# Creating a local StreamingContext with three working thread and batch intervel of 1 sec
sc = SparkContext("local[3]",appName="NetworkWordCount")
print("ssssss")

ssc = StreamingContext(sc,1)

print("ssssss2")

#create a DStream that will connect to hostname:port , such as localhost:9999
lines = ssc.socketTextStream("localhost",9999)

print("Lines Details ", lines)

#This lines DStream represents the stream of data that will be received from the data server. Each record in this DStream is a line of text. Next, we want to split the lines by space into words.
#split each line into words
words = lines.flatMap(lambda line: line.split(" "))
print("Words Details ", words)


#flatMap is a one-to-many DStream operation that creates a new DStream by generating multiple new records from each record in the source DStream. In this case, each line will be split into multiple words and the stream of words is represented as the words DStream. Next, we want to count these words.

#count each word in each batch
pairs = words.map(lambda word: (word,1))
wordCounts = pairs.reduceByKey(lambda x, y: x+y)
print("Word Count : ", wordCounts)

# Print the first ten elements of each RDD generated in this DStream to the console
print("****************************************************************************************************************************************************\n")
wordCounts.pprint()
print("****************************************************************************************************************************************************\n")

print("ssssss6  words total count ", wordCounts.count())

#The words DStream is further mapped (one-to-one transformation) to a DStream of (word, 1) pairs, which is then reduced to get the frequency of words in each batch of data. Finally, wordCounts.pprint() will print a few of the counts generated every second.

#Note that when these lines are executed, Spark Streaming only sets up the computation it will perform when it is started, and no real processing has started yet. To start the processing after all the transformations have been setup, we finally call

ssc.start()             # Start the computation

ssc.awaitTermination()  # Wait for the computation to terminate
'''
'''
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pickle, json

# Create a local StreamingContext and batch interval of 1 second
sc = SparkContext("local[*]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
socket_stream = ssc.socketTextStream("localhost", 10000)
result = socket_stream.map(lambda x: json.loads(x))
result.pprint()
ssc.start()
ssc.awaitTermination()

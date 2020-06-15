import _thread as thread
from spark_streaming import run_spark
from tweet_server import run_server

exitFlag = 0


# Create two threads as follows
thread.start_new_thread(run_spark, ())
thread.start_new_thread(run_server, ())

while 1:
   pass

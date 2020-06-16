import _thread as thread
from spark_streaming import run_spark
from tweet_server import run_server
import os
import time
def start_zookeeper():
   os.system("./kafka/bin/zookeeper-server-start.sh ./kafka/config/zookeeper.properties")


def start_kafka():
   os.system("./kafka/bin/kafka-server-start.sh ./kafka/config/server.properties")


def create_kafka_topic():
   os.system("./kafka/bin/kafka-topics.sh --create --zookeeper localhost:10001 --replication-factor 1 --partitions 1 --topic twitter")
   os.system("./kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic twitter --config retention.ms=5000")


if __name__ == "__main__":

   thread.start_new_thread(start_zookeeper, ())
   #time.sleep(1)
   thread.start_new_thread(start_kafka, ())
   #time.sleep(2)
   thread.start_new_thread(create_kafka_topic, ())
   #time.sleep(2)
   #thread.start_new_thread(run_spark, ())
   #thread.start_new_thread(run_server, ())

   while 1:
      pass

import _thread as thread
from spark_streaming import run_spark
from tweet_server import run_server
import os


# create function to start and stop mongodb server
def start_mongod():
    os.system('brew services start mongodb-community@4.2')


def stop_mongod():
    os.system('brew services stop mongodb-community@4.2')


if __name__ == '__main__':

    try:
        thread.start_new_thread(start_mongod, ())
        thread.start_new_thread(run_spark, ())
        thread.start_new_thread(run_server, ())

        while 1:
            pass

    finally:
        stop_mongod()

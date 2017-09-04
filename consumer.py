from daemon import Daemon
import sys
from daemon import Daemon
from kafka import KafkaConsumer
import json
import os

class Consumer(Daemon):


    def run(self):
        print('runnn')
        dir_path = '/Users/phani/Projects/Data_Science/spark-twitter'
        file_path = os.path.join(dir_path, 'tweets.json')
        print(dir_path)
        consumer = KafkaConsumer('twitter',
                                 bootstrap_servers=['localhost'],
                                 group_id='kafka-python-client',
                                 auto_offset_reset='earliest')

        for message in consumer:
            consumer.commit()
            with open(file_path, 'a') as outfile:
                outfile.write(message.value.decode('utf-8'))
                outfile.write("\n")


if __name__ == "__main__":
    #print(os.path.dirname(os.path.abspath(__file__)))
    daemon = Consumer('/tmp/stream-consumer.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(2)

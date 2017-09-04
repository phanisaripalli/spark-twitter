from daemon import Daemon
import sys
import config
from stream_listener import StreamListener
import tweepy

class Producer(Daemon):
    def __init__(self, pid_file):
        """
        Constructor
        """
        Daemon.__init__(self, pid_file)
        self.search_key = 'usopen'

    def run(self):
        self.start_stream()

    def start_stream(self):
        consumer_key = config.consumer_key
        consumer_secret = config.consumer_secret
        access_key = config.access_key
        access_secret = config.access_secret

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_key, access_secret)
        stream_listener = StreamListener()
        stream_listener.set_stream(self)
        stream_listener = tweepy.Stream(auth=auth, listener=stream_listener)
        stream_listener.filter(track=[self.search_key], languages=['en'])

if __name__ == "__main__":
    daemon = Producer('/tmp/stream-producer.pid')
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

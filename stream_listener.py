import tweepy
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

class StreamListener(tweepy.StreamListener):


    def on_status(self, status):
        message = {'id': status.id,
                   'created_at': str(status.created_at),
                   'text': status.text,
                   'user': status.user.screen_name,
                   'user_id': status.user.id,
                   'retweeted': status.retweeted,
                   'hashtags': status.entities['hashtags'],
                   'time': int(time.time()),
                   'search_key': self.stream.search_key}

        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        producer.send('twitter', json.dumps(message).encode('utf-8')).get(timeout=30)
        created_at = str(status.created_at)
        #print('Tweeet at ' + str(created_at))

        return True


    def get_stream(self, auth, stream_listener):
        return tweepy.Stream(auth=auth, listener=stream_listener)

    def on_error(self, status_code):
        print('An error has occurred! Status = %s' % status_code)
        return True  # Don't kill the stream

    def set_stream(self, t):
        """
        Set Stream class object - for producer etc.
        """
        self.stream = t
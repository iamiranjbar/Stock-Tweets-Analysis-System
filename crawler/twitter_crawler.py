# -*- coding: utf-8 -*-import twitter_config

from tweepy import OAuthHandler,Stream
from tweepy.streaming import StreamListener
import json
from twitter_config import consumer_key, consumer_secret, access_token, access_secret

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

class TweetListener(StreamListener):

    def on_data(self, data):

            json_data = json.loads(data)
            print("Original Data : ")

            print("Tweet ID : "+str(json_data["id"]))
            print(" User ID : "+str(json_data["user"]["id"]))
            print(" User Name : " + json_data["user"]["name"])
            print(" Tweet Text :"+json_data["text"])
            print(" Tweet Date :"+json_data["created_at"])
            print("User Info : \n" +json.dumps(json_data["user"]))
            print("-" * 20)
            print("Original Data : ")
            print(data)
            print("-" * 20)

            return True

    def on_error(self, status):
        print(status)
        return True

twitter_stream = Stream(auth, TweetListener())
twitter_stream.filter(languages=['fa'])#, track=[u'با' , u'از',u'به',u'در'])

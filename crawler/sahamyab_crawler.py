import requests, json
import time
from bson import json_util
from kafka import KafkaProducer

last_ids = []

def get_tweets():
    try:
        response = requests.get("https://www.sahamyab.com/guest/twiter/list?v=0.1", headers={"User-Agent": "Chrome/61"})
        data = json.loads(response.text)
        tweets = data["items"]
        return tweets
    except Exception as error:
        print(error)
        return []

producer = KafkaProducer()

while True:
    tweets = get_tweets()
    for tweet in tweets:
        if tweet[u'id'] in last_ids:
            continue
        else:
            producer.send('sahamyab_raw', json.dumps(tweet, default=json_util.default).encode('utf-8'))
            print('sent new tweet to kafka')
            last_ids.append(tweet[u'id'])
            if len(last_ids) > 20:
                del last_ids[0]
    time.sleep(60)

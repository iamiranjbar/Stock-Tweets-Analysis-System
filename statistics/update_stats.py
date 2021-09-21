import json
import redis
import datetime
from bson import json_util
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer('statistics')
producer = KafkaProducer()

def run():
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    
    for message in consumer:
        tweet = json.loads(message.value)
        try:
            hashtags = tweet["hashtags"]
            send_time = tweet["sendTimePersian"]
            date, time = send_time.split()
            year, month, day = date.split('/')
            hour, _ = time.split(':')
            expiration_time_delta = datetime.timedelta(days=7)

            # Tweets count for special hashtags
            for hashtag in hashtags:
                key = f"{hashtag}_{year}_{month}_{day}_{hour}"
                if redis_client.exists(key):
                    old_value = int(redis_client.get(key))
                    redis_client.set(key, old_value+1)
                else:
                    redis_client.set(key, 1, ex=expiration_time_delta)
            
            # Tweets count for special time
            key = f"post_{year}_{month}_{day}_{hour}"
            if redis_client.exists(key):
                old_value = int(redis_client.get(key))
                redis_client.set(key, old_value+1)
            else:
                redis_client.set(key, 1, ex=expiration_time_delta)
            
            # Unique hashtags count for special time
            key = f"hashtags_{year}_{month}_{day}_{hour}"
            for hashtag in hashtags:
                redis_client.sadd(key, hashtag)
                redis_client.expire(key, expiration_time_delta)

            # Last 1000 hastags
            key = "last_1000_hashtags"
            for hashtag in hashtags:
                redis_client.lpush(key, hashtag)
            redis_client.ltrim(key, 0, 999)
            redis_client.expire(key, expiration_time_delta)

            # Last 100 tweets
            key = "last_100_tweets"
            dumped_json = json.dumps(tweet, default=json_util.default).encode('utf-8')
            redis_client.lpush(key, dumped_json)
            redis_client.ltrim(key, 0, 99)
            redis_client.expire(key, expiration_time_delta)

            print("Statistics has been updated with tweet {}.".format(tweet["id"]))
            producer.send('analytics', json.dumps(tweet, default=json_util.default).encode('utf-8'))
            print('Sent preproccesed data to analytics channel.')
        except Exception as e:
            print(e)

if __name__=="__main__":
    run()

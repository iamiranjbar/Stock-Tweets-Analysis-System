import json
import redis
import jdatetime
from flask import Flask
from flask import request

app = Flask(__name__)

redis_client = redis.Redis(host='localhost', port=6379, db=0, charset="utf-8", decode_responses=True)

def calculate_past_date_time(year, month, day, hour, hours_past):
    date_times = []
    for hours_diff in range(hours_past):
        new_hour = hour - hours_diff
        new_day = day
        # TODO: Can implement for previous month and year with a calendar corner cases
        if new_hour < 0:
            new_hour += new_hour + 24
            new_day -= 1
        new_hour = f"{new_hour:02}"
        new_day = f"{new_day:02}"
        month = f"{int(month):02}"
        date_time = f"{year}_{month}_{new_day}_{new_hour}"
        date_times.append(date_time)
    return date_times

@app.route('/hashtag_tweets_count/<hashtag>')
def get_tweets_count_hashtag(hashtag):
    past_hours = 6
    now = jdatetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour

    past_date_times = calculate_past_date_time(year, month, day, hour, past_hours)

    result = 0
    for past_date_time in past_date_times:
        key = f"{hashtag}_{past_date_time}"
        encoded_key = key.encode('utf8')
        if redis_client.exists(encoded_key):
            result += int(redis_client.get(encoded_key))
    return f"Number of tweets for #{hashtag} in the last 6 hours: {result}"

@app.route('/hours_tweets_count/<int:hours_number>')
def get_tweets_count_past_hour(hours_number):
    now = jdatetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour

    past_date_times = calculate_past_date_time(year, month, day, hour, hours_number)

    result = 0
    for past_date_time in past_date_times:
        key = f"post_{past_date_time}"
        encoded_key = key.encode('utf8')
        if redis_client.exists(encoded_key):
            result += int(redis_client.get(encoded_key))
    return f"Number of tweets in the last {hours_number} hours: {result}"

@app.route('/hashtags_count/<int:hours_number>')
def get_unique_hashtags_count_past_hour(hours_number):
    now = jdatetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour

    past_date_times = calculate_past_date_time(year, month, day, hour, hours_number)

    result = 0
    for past_date_time in past_date_times:
        key = f"hashtags_{past_date_time}"
        encoded_key = key.encode('utf8')
        if redis_client.exists(encoded_key):
            result += len(redis_client.smembers(encoded_key))
    return f"Number of unique hashtags in the last {hours_number} hours: {result}"

@app.route('/last_1000_hashtags')
def get_last_1000_hashtags():
    key = "last_1000_hashtags"
    hashtags = redis_client.lrange(key, 0, -1)
    result = "The last 1000 hashtags:<br>"
    for hashtag in hashtags:
        result += f"{hashtag}<br>"
    return result

@app.route('/last_100_tweets')
def get_last_100_tweets():
    key = "last_100_tweets"
    tweets = [json.loads(tweet) for tweet in redis_client.lrange(key, 0, -1)]
    result = "The last 100 tweets:<br>"
    for tweet in tweets:
        result += f"{tweet}<br><br>"
    return result

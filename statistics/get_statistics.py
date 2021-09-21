import json
import redis
import jdatetime

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
    return result

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
    return result

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
    return result

def get_last_1000_hashtags():
    key = "last_1000_hashtags"
    hashtags = redis_client.lrange(key, 0, -1)
    return hashtags

def get_last_100_tweets():
    key = "last_100_tweets"
    tweets = [json.loads(tweet) for tweet in redis_client.lrange(key, 0, -1)]
    return tweets

def run():
    print("1. Tweets count for special hashtag in last 6 hour")
    print("2. Tweets count for last n hours")
    print("3. Unique Hashtags count for last n hours")
    print("4. Last 1000 hashtags")
    print("5. Last 100 tweets")
    command = int(input("Please enter your command (1-5): "))
    
    if command == 1:
        hashtag = input("Please enter a symbol in persian: ")
        result = get_tweets_count_hashtag(hashtag)
        print(f"Tweets count for {hashtag} in last 6 hour: {result}")
    elif command == 2:
        hours_number = int(input("Please enter how many past hours: "))
        result = get_tweets_count_past_hour(hours_number)
        print(f"Tweets count for last {hours_number} hours: {result}")
    elif command == 3:
        hours_number = int(input("Please enter how many past hours: "))
        result = get_unique_hashtags_count_past_hour(hours_number)
        print(f"Unique Hashtags count for last {hours_number} hours: {result}")
    elif command == 4:
        result = get_last_1000_hashtags()
        print("1000 recent hashtags:")
        print(result)
    elif command == 5:
        result = get_last_100_tweets()
        print("100 recent tweets:")
        print(result)
    else:
        print("Wrong command!")

if __name__=="__main__":
    run()

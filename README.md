# BigData Spring2021 Final Project

## Getting Started

Use step 1 and 2 from this guide https://kafka.apache.org/quickstart to start zookeeper and kafka.

Create the topics with the following code (you can change zookeeper adress or any parameter according to your usage):
```bash
bin/kafka-topics.sh --create --topic sahamyab_raw --partitions 2 --replication-factor 1 --zookeeper localhost:2181
bin/kafka-topics.sh --create --topic persistence --partitions 2 --replication-factor 1 --zookeeper localhost:2181
```
Install requirements.txt with following command:
```
pip3 install -r requirements.txt
```

## Crawl data

Run the sahamyab_crawler.py with `python3 crawler/sahamyab_crawler.py`. This is the producer which tries to read new data every minute and will send the new data to kafka `sahamyab_raw` topic.

## Preproccess data

Run the preprocess.py with `python3 preproccess/preprocess.py`. This will consume the data from kafka, and will process the data to find keywords and hashtags. Then, it will produce the data to `persistence` topic.

## Persist data

Download  Elastic search and Kibana and start them through this tutorial: https://yun.ir/fi9loe

After that, Run add tweets to elastic search script with `python3 persistence/add_tweets.py`. This script, add preproccessed data to elastic search and `statistics` kafka topic for next stage.

Create related plots in kibana and see online plots after that.

## Calculate statistics

First of all, download redis and install it from this tutorial: https://redis.io/topics/quickstart
After installing, start `redis-server`.

After that, run `python3 statistics/update_stats.py` to update key values in redis that contains statics for different hashtags and tweets.

Given queries can be answer by created keys. `get_statistics.py` implements main queries. It has a interactive cli for running this queries. You can play with it by running `python3 statistics/get_statistics.py`.

We use function of this stage in flask web server for getting data. So if you need you can import them:)

## Running Flask web application for calculating statistics

For running the flask application, you should first run the following to determine the flask application and environment:

```bash
export FLASK_APP=get_statistics_web_app
export FLASK_ENV=development
```

Now, you can run serve the queries on the web app by running the following command in the `./statistics` directory.
```bash
flask run
```
Then, you can check the responses on localhost:5000.

## Analytics

First, we need to start the clickhouse driver. You can do this by following instructions in here: https://clickhouse.tech/docs/en/getting-started/install/

Then, you can run the code that adds every tweet from analytics kafka topic to the clickhouse DB by running `analytics/clickhouse.py`.

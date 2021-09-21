import json
from bson import json_util
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer('persistence')
producer = KafkaProducer()
elastic_search = Elasticsearch([{'host': 'localhost', 'port': 9200}])

for message in consumer:
    tweet = json.loads(message.value)
    try:
        elastic_search.index(index="sahamyab", body=tweet)
        print("Tweet {} has been imported to elastic search.".format(tweet["id"]))
        producer.send('statistics', json.dumps(tweet, default=json_util.default).encode('utf-8'))
        print('Sent preproccesed data to statistics channel.')
    except Exception as e:
        print(e)

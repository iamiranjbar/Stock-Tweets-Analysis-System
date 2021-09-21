# -*- coding: utf-8 -*-

import json
import uuid
import time
import re
from math import log, ceil
from bson import json_util
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer('sahamyab_raw')
producer = KafkaProducer()
DOCUMENT_FREQUENCY = {}
DOCS_COUNT = 0

def get_hashtags(text):
    words = [] if text is None else text.split()
    return list(set(word[1:] for word in words if word[0]=='#'))

def get_links(text):
    words = [] if text is None else text.split()
    return list(set(word for word in words if word[:8]==('https://') or word[:7]==('http://')))

def calculate_tf_idf(term_frequencies):
    tf_idf_scores = {}
    for word, term_frequency in term_frequencies.items():
        tf = log(1+term_frequency)
        idf = log(DOCS_COUNT/len(DOCUMENT_FREQUENCY[word]))
        tf_idf_scores[word] = tf * idf
    return tf_idf_scores

def get_keywords(text, id):
    given_keywords = ["بورس", "اقتصاد", "تحریم", "دولت", "حسن روحانی", "انتخابات", 
                       "دلار", "طلا", "کرونا", "کوید۱۹", "کوید ۱۹", "تورم", "دانشگاه", "covid19"]
    words = text.split()

    keywords = set()
    for word in words:
        if word in given_keywords:
            keywords.add(word)

    cleaned_words = []
    term_frequency = {}
    words = re.split('[ ,.;]', text)
    for word in words:
        if word not in stop_words and word != '':
            cleaned_words.append(word)
            if word not in DOCUMENT_FREQUENCY:
                DOCUMENT_FREQUENCY[word] = {id}
            else:
                if id not in DOCUMENT_FREQUENCY[word]:
                    DOCUMENT_FREQUENCY[word].add(id)
            if word not in term_frequency:
                term_frequency[word] = 1
            else:
                term_frequency[word] += 1
    
    tf_idf_scores = calculate_tf_idf(term_frequency)
    sorted_scores = dict(sorted(tf_idf_scores.items(), key = lambda x: x[1], reverse=True))
    selected_count = ceil(len(sorted_scores) / 10)
    added_count = 0
    for word, _ in sorted_scores.items():
        if added_count == selected_count:
            break
        keywords.add(word)
        added_count += 1
    return list(keywords)

with open("preprocess/persian") as stop_words_file:
        stop_words = stop_words_file.readlines()
        stop_words = [stop_word.strip() for stop_word in stop_words]

for message in consumer:
    DOCS_COUNT += 1
    print("%s:%d:%d" % (message.topic, message.partition, message.offset))
    tweet = json.loads(message.value)
    tweet['uuid'] = uuid.uuid4().hex
    tweet['received_timestamp'] = int(time.time() * 1000)
    content = tweet.get(u'content')
    if content is None:
        continue
    tweet['hashtags'] = get_hashtags(content)
    tweet['links'] = get_links(content)
    tweet['keywords'] = get_keywords(content, tweet['uuid'])
    producer.send('persistence', json.dumps(tweet, default=json_util.default).encode('utf-8'))
    print('Sent preproccesed data to persistence channel.')

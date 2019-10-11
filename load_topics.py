#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка топиков и топиков в контексты
"""
from collections import Counter
from functools import partial
import json
from operator import itemgetter

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

TOPICS = 'topic_output.json'
MONGO_URI = 'mongodb://localhost:27017/'
# MONGO_URI = 'mongodb://frigate:27017/'


def main():
  client = MongoClient(MONGO_URI, compressors='snappy')
  mdb = client['cirtec']
  # mpubs = mdb['publications']
  # mpubs_insert = mpubs.insert_one
  mtops = mdb['topics']
  mtops_update = partial(mtops.update_one, upsert=True)
  mcont = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)
  # mauth = mdb['authors']
  # mauth_update = partial(mauth.update_one, upsert=True)

  topics = json.load(open(TOPICS, encoding='utf-8'))
  tlp = topics['linked_papers']

  i = 0
  for i, topic in enumerate(tlp['topics'], 1):
    title = topic.pop('topic')
    topic['title'] = title
    topic['number'] = int(topic['number'])
    topic['probability_average'] = float(topic['probability_average'])
    topic['probability_std'] = float(topic['probability_std'])
    mtops_update(dict(_id=title), {'$set': topic})
  print('topic:', i)

  cnts = Counter()
  i = 0

  get_as_tuple = itemgetter('ref_key', 'topic', 'probability')
  for i, cont in enumerate(tlp['contexts'], 1):
    ref_key, topic, probab = get_as_tuple(cont)
    pub_id, num, start = ref_key.rsplit('_', 2)
    cont_id = f'{pub_id}@{start}'
    mcont_update(dict(_id=cont_id), {'$set': {'ref_num': int(num)}})
    mtops_update(dict(_id=topic),
      {'$addToSet': {
        'linked_papers': {'cont_id': cont_id, 'probability': float(probab)}}})

  print(i, len(cnts))


if __name__ == '__main__':
  main()

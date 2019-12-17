#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка топиков и топиков в контексты
"""
from collections import Counter
from datetime import datetime
from functools import partial, reduce
import json
from operator import itemgetter
from urllib.request import urlopen

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from load_utils import rename_new_field
from utils import load_config


# TOPICS = 'topic_output.json'
TOPICS:str = 'http://onir2.ranepa.ru:8081/prl/data/Sergey-Sinelnikov-Murylev/topic_output.json'


def main():
  start = datetime.now()
  conf = load_config()
  conf_mongo = conf['dev']['mongodb']
  for_del: int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    mtops, = update_topics(mdb, for_del, TOPICS)

    r = mtops.delete_many({'for_del': for_del})
    print(f'delete {mtops.name}:', r.deleted_count)


def update_topics(mdb:Database, for_del:int, uri_topics:str):
  """Обновление коллекции topics и дополнение в контексты"""

  mtops = mdb['topics']
  mtops_update = partial(mtops.update_one, upsert=True)
  mcont = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)

  mtops.update_many({}, {'$set': {'for_del': for_del}})
  mcont.update_many({}, {'$unset': {'linked_papers_topics': 1}})

  # topics = json.load(open(uri_topics, encoding='utf-8'))
  with urlopen(uri_topics) as f:
    topics = json.load(f)

  tlp = topics['linked_papers']
  i = 0
  for i, topic in enumerate(tlp['topics'], 1):
    title = topic.pop('topic')
    topic['title'] = title
    topic['number'] = int(topic['number'])
    topic['probability_average'] = float(topic['probability_average'])
    topic['probability_std'] = float(topic['probability_std'])
    mtops_update(dict(_id=title), {'$set': topic, '$unset': {'for_del': 1}})
  print('topic:', i)
  cnts = Counter()
  i = 0
  get_as_tuple = itemgetter('ref_key', 'topic', 'probability')
  for i, cont in enumerate(tlp['contexts'], 1):
    ref_key, topic, probab = get_as_tuple(cont)
    pub_id, num, start = ref_key.rsplit('_', 2)
    cont_id = f'{pub_id}@{start}'
    mcont_update(
      dict(_id=cont_id), {
      '$set': {'ref_num': int(num)}, '$addToSet': {
        'linked_papers_topics_new': {
          '_id': topic, 'probability': float(probab)}}})

  print(i, len(cnts))

  rename_new_field(mcont, 'linked_papers_topics')

  return (mtops,)


if __name__ == '__main__':
  main()

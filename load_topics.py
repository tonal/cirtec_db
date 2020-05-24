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
from typing import Iterable
from urllib.request import urlopen

from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.database import Database

from load_utils import AUTHORS, rename_new_field
from utils import load_config


# TOPICS = 'topic_output.json'
TOPICS:str = 'http://onir2.ranepa.ru:8081/prl/data/%(author)s/topic_output.json'


def main():
  start = datetime.now()
  conf = load_config()
  conf_mongo = conf['dev']['mongodb']
  for_del: int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    mtops, = update_topics(mdb, AUTHORS, for_del, TOPICS)

    r = mtops.delete_many({'for_del': for_del})
    print(f'delete {mtops.name}:', r.deleted_count)


def update_topics(
  mdb:Database, authors:Iterable[str], for_del:int, uri_topics:str
):
  """Обновление коллекции topics и дополнение в контексты"""

  mtops = mdb['topics']
  mtops_replace = partial(
    mtops.find_one_and_replace, upsert=True,
    return_document=ReturnDocument.AFTER)
  mcont = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)

  mtops.create_index(
    [
      ('title', ASCENDING), ('uni_author', ASCENDING), ('uni_cited', ASCENDING),
      ('uni_citing', ASCENDING)],
    unique=True)
  mtops.update_many({}, {'$set': {'for_del': for_del}})
  # mcont.update_many({}, {'$unset': {'linked_papers_topics': 1}})

  cnt_t = cnt_r = 0
  for uni_author in authors:
    cr, ct = load_topics_json(
      mcont_update, mtops_replace, uri_topics, uni_author)
    cnt_r += cr
    cnt_t += ct

  print('all', cnt_t, cnt_r)

  rename_new_field(mcont, 'topics')
  mcont.update_many(
    {"linked_papers_topics": {"$exists": 1}},
    {"$unset": {"linked_papers_topics": 1}})

  return (mtops,)


def load_topics_json(mcont_update, mtops_replace, uri_topics, uni_author):
  with urlopen(uri_topics % dict(author=uni_author)) as f:
    topics = json.load(f)
  # tlp = topics['linked_papers']
  cnt_t = cnt_r = 0
  for name, tlp in topics.items():
    uni_field = name.split('_', 1)[0]
    uni_field = 'uni_author' if uni_field == 'linked' else ('uni_' + uni_field)
    t2oid = {}
    i = 0
    for i, topic in enumerate(tlp['topics'], 1):
      title = topic.pop('topic')
      # topic['title'] = ', '.join(sorted(map(str.strip, title.split(','))))
      topic['words'] = words = tuple(map(str.strip, title.split(',')))
      topic['title'] = title = ', '.join(words)
      topic['number'] = int(topic['number'])
      topic['probability_average'] = float(topic['probability_average'])
      topic['probability_std'] = float(topic['probability_std'])
      topic[uni_field] = uni_author
      obj = mtops_replace({'title': title, uni_field: uni_author}, topic)
      assert obj['_id'], (dict(title=title, uni_field=uni_author), obj)
      t2oid[title] = obj['_id']
    print(uni_author, name, 'topic:', i)
    cnt_t += i
    cnts = Counter()
    i = 0
    get_as_tuple = itemgetter('ref_key', 'topic', 'probability')
    for i, cont in enumerate(tlp['contexts'], 1):
      ref_key, topic, probab = get_as_tuple(cont)
      # topic = ', '.join(sorted(map(str.strip, topic.split(','))))
      topic = ', '.join(map(str.strip, topic.split(',')))
      oid = t2oid[topic]
      pub_id, num, start = ref_key.rsplit('_', 2)
      cont_id = f'{pub_id}@{start}'
      mcont_update(dict(_id=cont_id), {
        '$set': {'ref_num': int(num)}, '$addToSet': {
          'topics_new': {
            '_id': oid, 'title': topic, 'probability': float(probab)}}})

    print(name, i, len(cnts))
    cnt_r += i
  return cnt_r, cnt_t


if __name__ == '__main__':
  main()

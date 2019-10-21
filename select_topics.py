#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""
from collections import Counter, defaultdict

from pymongo import MongoClient

from utils import load_config


AUTHOR = 'Oates'


def main():
  conf = load_config()
  conf_mongo = conf['mongodb']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    print_freq_topics_by_frags(mdb.topics, 100)
    print()
    print_author_topics_by_frags(mdb.contexts, AUTHOR)


def print_author_topics_by_frags(contexts, author:str):
  topics = {}
  topics_cnt = Counter()
  coaut = defaultdict(Counter)
  for i, doc in enumerate(
    contexts.aggregate([
      {'$match': {'cocit_authors': author}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$lookup': {
        'from': 'topics', 'localField': '_id',
        'foreignField': 'linked_papers.cont_id', 'as': 'tops'}},
      {'$project': {'cocit_authors': True, 'tops': True}}
    ]),
    1
  ):
    # print(i, doc)
    coauthors = frozenset(c for c in doc['cocit_authors'] if c != AUTHOR)
    for topic in doc['tops']:
      title = topic['title']
      topics_cnt[title] += 1
      if title not in topics:
        topics[title] = topic

      for ca in coauthors:
        coaut[ca][title] += 1
  print(f"'{AUTHOR}' co-cited with topics:")
  sort_key = key = lambda kv: (-kv[1], kv[0])
  for n, i in sorted(topics_cnt.items(), key=sort_key):
    msg = (f'{i} '
           f'avg: {topics[n]["probability_average"]} '
           f'std: {topics[n]["probability_std"]} '
           f'"{n}"')
    print(msg)
  print('including by co-cited authors:')
  for i, (co, cnts) in enumerate(
    sorted(coaut.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1):
    # print(i, co, cnts)
    # print(
    #   f"  {i} '{co}': "
    #   f"{', '.join('in fragment %s repeats: %s' % (f, c) for f, c in cnts.items())}")
    print(' ', i, f"'{co}'")
    for t, c in sorted(cnts.items(), key=sort_key):
      print('  ', c, f'"{t}"')


def print_freq_topics_by_frags(topics, freq:int=100):
  for doc in topics.aggregate([
    {'$unwind': '$linked_papers'},
    {'$group': {
      '_id': '$title', 'count': {'$sum': 1},
      'conts': {'$addToSet': '$linked_papers.cont_id'}}},
    {'$match': {'count': {'$gte': freq}}}, # {'$project': {'count': false}},
    {'$unwind': '$conts'},
    {'$lookup': {
      'from': 'contexts', 'localField': 'conts', 'foreignField': '_id',
      'as': 'context'}},
    {'$unwind': '$context'},
    {'$match': {'context.frag_num': {'$gt': 0}}},
    {'$group': {
      '_id': '$context.frag_num', 'count': {'$sum': 1},
      'topics': {'$addToSet': '$_id'}}},
    {'$sort': {'_id': 1}}
  ]):
    print('fragment:', doc['_id'], 'cnt:', doc['count'])
    for t in sorted(doc['topics']):
      print(f'  "{t}"')


if __name__ == '__main__':
  main()

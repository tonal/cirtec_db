#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""
from collections import Counter, defaultdict

from pymongo import MongoClient


MONGO_URI = 'mongodb://localhost:27017/'

AUTHOR = 'Oates'


def main():
  client = MongoClient(MONGO_URI, compressors='snappy')
  mdb = client['cirtec']

  print_freq_ngramms_by_frags(mdb.n_gramms, 50, nka=2)
  print()
  print_author_ngramms_by_frags(mdb.contexts, AUTHOR)


def print_freq_ngramms_by_frags(
  n_gramms, freq:int=100, *, nka:int=None, ltype:str=None
):
  if nka and ltype:
    pipeline = [{'$match': {'nka': nka, 'type': ltype}}, ]
  elif nka:
    pipeline = [{'$match': {'nka': nka}}, ]
  elif ltype:
    pipeline = [{'$match': {'type': ltype}}, ]
  else:
    pipeline = []
  pipeline += [
    {'$unwind': '$linked_papers'},
    {'$group': {
      '_id': '$title', 'count': {'$sum': '$linked_papers.cnt'},
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
      'n_gramms': {'$addToSet': '$_id'}}},
    {'$sort': {'_id': 1}}]
  for doc in n_gramms.aggregate(pipeline):
    print('fragment:', doc['_id'], 'cnt:', doc['count'])
    for t in sorted(doc['n_gramms']):
      print(f'  "{t}"')


def print_author_ngramms_by_frags(contexts, author:str):
  ngramms = {}
  topics_cnt = Counter()
  coaut = defaultdict(Counter)
  for i, doc in enumerate(
    contexts.aggregate([
      {'$match': {'cocit_authors': 'Oates'}},
      {'$project': {
        'exact': False, 'prefix': False, 'suffix': False, 'topics': False}},
      {'$lookup': {
        'from': 'n_gramms', 'localField': '_id',
        'foreignField': 'linked_papers.cont_id', 'as': 'ngramms'}},
      # {$unwind: '$ngramms'},
      {'$project': {'ngramms.linked_papers': False}},
    ]), 1
  ):
    # print(i, doc)
    coauthors = frozenset(c for c in doc['cocit_authors'] if c != author)
    for ngramm in doc['ngramms']:
      title = ngramm['title']
      topics_cnt[title] += 1
      if title not in ngramms:
        ngramms[title] = ngramm

      for ca in coauthors:
        coaut[ca][title] += 1
  print(f"'{author}' co-cited with topics:")
  sort_key = key = lambda kv: (-kv[1], kv[0])
  for n, i in sorted(topics_cnt.items(), key=sort_key):
    msg = (f'  {i} '
           f'cnt: {ngramms[n]["count_in_linked_papers"]} '
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


if __name__ == '__main__':
  main()

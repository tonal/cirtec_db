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

  frags = Counter()
  coaut = defaultdict(Counter)


  for i, doc in enumerate(
    mdb.contexts.find(
      {'cocit_authors': AUTHOR, 'frag_num': {'$gt': 0}},
      projection=['frag_num', 'cocit_authors']
    ).sort('frag_num'),
    1
  ):
    # print(i, doc)
    fnum = doc['frag_num']
    coauthors = frozenset(c for c in doc['cocit_authors'] if c != AUTHOR)
    frags[fnum] += 1
    for ca in coauthors:
      coaut[ca][fnum] += 1

  msg = f'{"/".join(str(frags[i]) for i in range(1, 6))}'
  print(f"'{AUTHOR}' co-cited in fragments {msg}") #, i)


  for i, (co, cnts) in enumerate(
    sorted(
      coaut.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])),
    1
  ):
    # print(i, co, cnts)
    print(
      f"  {i} '{co}': "
      f"{', '.join('in fragment %s repeats: %s' % (f, c) for f, c in cnts.items())}")


if __name__ == '__main__':
  main()

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

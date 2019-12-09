#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка файла со-цитированных авторов
"""
from collections import Counter
from datetime import datetime
from functools import partial, reduce
import json
from pathlib import Path
from urllib.request import urlopen

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from utils import load_config

COCITS_FILE:str = 'linked_papers_cocits_aunas.json'
COCITS:str = f'file://{Path(COCITS_FILE).resolve()}'


def main():
  start = datetime.now()
  conf = load_config()
  conf_mongo = conf['dev']['mongodb']
  for_del: int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])
  fcocits = COCITS

  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    _ = update_cocits(mdb, for_del, fcocits)
    # r = mcont.find({'cocit_authors': {'$exists': True}}).count()
    # print(f'delete {mcont.name}:', r)


def update_cocits(mdb, for_del, uri_cocits):
  now = datetime.now
  mpubs = mdb['publications']
  mpubs_insert = mpubs.insert_one
  # mpubs_insert = partial(mpubs.update_one, upsert=True)
  mcont = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)

  # cocits = json.load(open(fcocits, encoding='utf-8'))
  with urlopen(uri_cocits) as f:
    cocits = json.load(f)

  cnt = i = j = k = 0
  cont = author = coauthor = ''
  cnts = Counter()
  pubs = set()

  for i, (author, cocits2) in enumerate(cocits.items(), 1):
    # print(i, author)

    j = 0
    for j, (coauthor, conts) in enumerate(cocits2.items(), 1):
      # print(' ', j, coauthor, len(conts))

      k = 0
      for k, cont in enumerate(conts, 1):
        cnts[author] += 1
        cnts[coauthor] += 1
        cnt += 1
        pub_id, start = cont.split('@')
        # print('   ', k, pub_id, start)
        mcont_update(dict(_id=f'{pub_id}@{start}'),
          {'$addToSet': {'cocit_authors_new': {'$each': [author, coauthor]}},
           '$set': {'pub_id': pub_id, 'start': start},})
        try:
          if pub_id not in pubs:
            mpubs_insert(dict(_id=pub_id))
            pubs.add(pub_id)
        except DuplicateKeyError:
          pass

        if cnt % 100 == 0:
          print(now(), cnt, i, j, k, cont, author, coauthor)

  print(now(), cnt, i, j, k, cont, author, coauthor)
  print(now(), cnt, len(cnts))
  # mcont.update_many(
  #   {'$or': [
  #     {'cocit_authors': {'$exists': True}},
  #     {'cocit_authors_new': {'$exists': True}}]},
  #   {
  #     '$rename': {
  #       'cocit_authors': 'cocit_authors_old',
  #       'cocit_authors_new': 'cocit_authors'},
  #     '$unset': {'cocit_authors_old': 1},
  #   })
  mcont.update_many(
    {'cocit_authors': {'$exists': True}},
    {'$rename': {'cocit_authors': 'cocit_authors_old'}})
  mcont.update_many(
    {'cocit_authors_new': {'$exists': True}},
    {'$rename': {'cocit_authors_new': 'cocit_authors'}})
  mcont.update_many(
    {'cocit_authors_old': {'$exists': True}},
    {'$unset': {'cocit_authors_old': 1}})
  return ()


if __name__ == '__main__':
  main()

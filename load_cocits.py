#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка файла со-цитированных авторов
"""
from collections import Counter
from datetime import datetime
from functools import partial, reduce
import json
from operator import itemgetter
from pprint import pprint
from typing import Sequence
from urllib.request import urlopen

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from load_utils import rename_new_field
from utils import load_config

# COCITS_FILE:str = 'linked_papers_cocits_aunas.json'
COCITS_AUTHORS:str = 'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/linked_papers_cocits_aunas.json'
COCITS_REFS:str = 'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/linked_papers_cocits_bunshids.json'

def main():
  start = datetime.now()
  conf = load_config()
  conf_mongo = conf['dev']['mongodb']
  # conf_mongo = conf['mongodb']
  for_del: int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    _ = update_cocits_authors(mdb, for_del, COCITS_AUTHORS)
    _ = update_cocits_refs(mdb, for_del, COCITS_REFS)
    # get_no_base(mdb, get_conames(COCITS_REFS))


def update_cocits_authors(mdb:Database, for_del:int, url:str):
  """Загрузка файла со-цитированных авторов"""
  return _update_cocits(mdb, for_del, 'cocit_authors', url)


def update_cocits_refs(mdb:Database, for_del:int, url:str):
  """Загрузка файла со-цитированных референсов"""
  return _update_cocits(mdb, for_del, 'cocit_refs', url)


def _update_cocits(mdb:Database, for_del:int, field:str, uri:str):
  """Загрузка файла со-цитирований"""

  now = datetime.now
  mpubs = mdb['publications']
  mpubs_insert = mpubs.insert_one
  # mpubs_insert = partial(mpubs.update_one, upsert=True)
  mcont = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)

  cnt = i = j = k = 0
  cnts = Counter()
  cont = author = coauthor = ''

  # cocits = json.load(open(fcocits, encoding='utf-8'))
  with urlopen(uri) as f:
    cocits = json.load(f)

  i = j = k = 0
  pubs = set()

  for i, (author, values) in enumerate(cocits.items(), 1):
    # print(i, author)
    if author == 'null':
      # в данных бывают артефакты
      continue
    coauthor = ''
    try:
      cocits2 = values['co']
    except KeyError:
      if author == 'TOTALS':
        break
      print('!!!', author, values)
      raise

    j = 0
    for j, (coauthor, conts) in enumerate(cocits2.items(), 1):
      # print(' ', j, coauthor, len(conts))
      if coauthor == 'null':
        # в данных бывают артефакты
        continue

      k = 0
      for k, cont in enumerate(conts, 1):
        cnts[author] += 1
        cnts[coauthor] += 1
        cnt += 1
        pub_id, start = cont.split('@')
        # print('   ', k, pub_id, start)
        mcont_update(dict(_id=f'{pub_id}@{start}'),
          {'$addToSet': {f'{field}_new': {'$each': [author, coauthor]}},
           '$set': {'pub_id': pub_id, 'start': int(start)},})
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

  rename_new_field(mcont, field)
  return ()


def get_conames(uri:str):
  with urlopen(uri) as f:
    cocits = json.load(f)

  conames = Counter()
  for i, (author, values) in enumerate(cocits.items(), 1):
    # print(i, author)
    try:
      cocits2 = values['co']
    except KeyError:
      if author == 'TOTALS':
        break
      print('!!!', author, values)
      raise

    conames[author] += 1
    for j, (coauthor, conts) in enumerate(cocits2.items(), 1):
      conames[coauthor] += 1

  for i, (n, c) in enumerate(conames.most_common(), 1):
    print(i, n, c)

  pprint(list(map(itemgetter(0), conames.most_common())), indent=2, compact=True)

  return tuple(conames.keys())


def get_no_base(mdb:Database, bundles_name:Sequence):
  bundles = mdb.bundles
  curs = bundles.find({'_id': {'$in': tuple(bundles_name)}})

  bd_bndl = frozenset(map(itemgetter('_id'), curs))
  print(frozenset(bundles_name) - bd_bndl)


if __name__ == '__main__':
  main()
  # get_conames(COCITS_REFS)

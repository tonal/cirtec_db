#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка файла со-цитированных авторов
"""
from collections import Counter
from datetime import datetime
from functools import partial, reduce
import json
from urllib.request import urlopen

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from loads.common import rename_new_field
from utils import load_config

# COCITS_FILE:str = 'linked_papers_cocits_aunas.json'
COCITS_AUTHORS = (
  'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/linked_papers_cocits_aunas.json',
  'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/cited_papers_cocits_aunas.json',
  'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/citing_papers_cocits_aunas.json',
)
COCITS_REFS = (
  'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/linked_papers_cocits_bunshids.json',
  'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/cited_papers_cocits_bunshids.json',
  'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/citing_papers_cocits_bunshids.json',
)

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


def update_cocits_authors(mdb:Database, for_del:int, urls):
  """Загрузка файла со-цитированных авторов"""
  return _update_cocits(mdb, for_del, 'cocit_authors', urls)


def update_cocits_refs(mdb:Database, for_del:int, urls):
  """Загрузка файла со-цитированных референсов"""
  return _update_cocits(mdb, for_del, 'cocit_refs', urls)


def _update_cocits(mdb:Database, for_del:int, field:str, uris):
  """Загрузка файла со-цитирований"""

  mpubs = mdb['publications']
  mpubs_insert = mpubs.insert_one
  # mpubs_insert = partial(mpubs.update_one, upsert=True)
  mcont = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)

  pubs = set()

  for uri in uris:
    load_json(mcont_update, mpubs_insert, field, uri, pubs)

  rename_new_field(mcont, field)
  return ()


def load_json(mcont_update, mpubs_insert, field, uri, pubs):
  now = datetime.now
  cnts = Counter()
  cont = author = coauthor = ''
  # cocits = json.load(open(fcocits, encoding='utf-8'))
  with urlopen(uri) as f:
    cocits = json.load(f)
  i = j = k = cnt = 0
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
        mcont_update(dict(_id=f'{pub_id}@{start}'), {
          '$addToSet': {f'{field}_new': {'$each': [author, coauthor]}},
          '$set': {'pub_id': pub_id, 'start': int(start)}, })
        try:
          if pub_id not in pubs:
            mpubs_insert(dict(_id=pub_id))
        except DuplicateKeyError:
          pass
        pubs.add(pub_id)

        if cnt % 100 == 0:
          print(now(), cnt, i, j, k, cont, author, coauthor)
  print(now(), cnt, i, j, k, cont, author, coauthor)
  print(now(), cnt, len(cnts))


if __name__ == '__main__':
  main()
  # get_conames(COCITS_REFS)

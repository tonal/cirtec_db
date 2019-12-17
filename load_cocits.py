#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка файла со-цитированных авторов
"""
from collections import Counter
from datetime import datetime
from functools import partial, reduce
import json
from typing import Sequence
from urllib.request import urlopen

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from load_utils import rename_new_field
from utils import load_config

# COCITS_FILE:str = 'linked_papers_cocits_aunas.json'
# COCITS:str = f'file://{Path(COCITS_FILE).resolve()}'
COCITS:Sequence[str] = (
  'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/linked_papers_cocits_aunas.json',
  'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/linked_papers_cocits_bunshids.json',
)

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


def update_cocits(mdb, for_del, uri_cocits:Sequence[str]):
  """Загрузка файла со-цитированных авторов"""
  now = datetime.now
  mpubs = mdb['publications']
  mpubs_insert = mpubs.insert_one
  # mpubs_insert = partial(mpubs.update_one, upsert=True)
  mcont = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)

  cnt = i = j = k = 0
  cnts = Counter()
  cont = author = coauthor = ''

  for uri in uri_cocits:
    # cocits = json.load(open(fcocits, encoding='utf-8'))
    with urlopen(uri) as f:
      cocits = json.load(f)

    i = j = k = 0
    pubs = set()

    for i, (author, values) in enumerate(cocits.items(), 1):
      # print(i, author)
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

  rename_new_field(mcont, 'cocit_authors')
  return ()


if __name__ == '__main__':
  main()

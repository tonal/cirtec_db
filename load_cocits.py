#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка файла со-цитированных авторов
"""
from collections import Counter
from functools import partial
import json

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from utils import load_config

COCITS = 'linked_papers_cocits_aunas.json'


def main():
  conf = load_config()
  conf_mongo = conf['mongodb']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    mpubs = mdb['publications']
    mpubs_insert = mpubs.insert_one
    mcont = mdb['contexts']
    mcont_update = partial(mcont.update_one, upsert=True)
    # mauth = mdb['authors']
    # mauth_update = partial(mauth.update_one, upsert=True)

    cnt = 0
    cnts = Counter()
    cocits = json.load(open(COCITS, encoding='utf-8'))
    for i, (author, cocits2) in enumerate(cocits.items(), 1):
      print(i, author)

      # mauth_update(dict(_id=author), {'$set': {'is_coauthors': True}})
      for j, (coauthor, conts) in enumerate(cocits2.items(), 1):
        print(' ', j, coauthor, len(conts))

        # mauth_update(dict(_id=coauthor), {'$set': {'is_coauthors': True}})
        for k, cont in enumerate(conts, 1):
          cnts[author] += 1
          cnts[coauthor] += 1
          cnt +=1
          pub_id, start = cont.split('@')
          print('   ', k, pub_id, start)
          mcont_update(
            dict(_id=f'{pub_id}@{start}'),
            {'$addToSet': {'cocit_authors': {'$each': [author, coauthor]}}})
          try:
            mpubs_insert(dict(_id=pub_id))
          except DuplicateKeyError:
            pass


    print(cnt, len(cnts))


if __name__ == '__main__':
  main()

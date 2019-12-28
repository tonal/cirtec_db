#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка контекстов цитирования
"""
from collections import Counter
from datetime import datetime
from functools import reduce
import re
from typing import Callable

from pymongo.database import Database
from pymongo import MongoClient
import requests

from load_classif_pos_neg import update_class_pos_neg
from load_pubs import update_pubs_conts, SOURCE_XML, SOURCE_URL
from load_bundles import update_bundles, BUNDLES
from load_cocits import (
  update_cocits_authors, COCITS_AUTHORS, update_cocits_refs, COCITS_REFS)
from load_ngrams import update_ngramms, NGRAM_ROOT
from load_topics import update_topics, TOPICS
from utils import load_config


def main():
  now = datetime.now
  start = now()
  print(start, 'start')
  for_del:int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  conf = load_config()['dev']
  conf_mongo = conf['mongodb']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb:Database = client[conf_mongo['db']]

    def do_upd(update:Callable, *args):
      print(now(), f'{update.__name__}: {update.__doc__}')
      ret = update(mdb, for_del, *args)
      return ret

    colls = tuple(
      c for u, *args in (
        (update_pubs_conts, SOURCE_URL, SOURCE_XML),
        (update_bundles, BUNDLES),
        (update_ngramms, NGRAM_ROOT),
        (update_topics, TOPICS),
        (update_cocits_authors, COCITS_AUTHORS),
        (update_cocits_refs, COCITS_REFS),
        (update_class_pos_neg,),
      )
      for c in do_upd(u, *args)
    )

    for coll in colls:
      r = coll.delete_many({'for_del': for_del})
      print(now(), f'delete {coll.name}:', r.deleted_count)

  print(now(), 'end')


def check_date():
  for uri in (
    SOURCE_URL, SOURCE_XML, BUNDLES, TOPICS, COCITS_AUTHORS, COCITS_REFS
  ):
    rsp = requests.head(uri)
    print(uri, rsp.status_code)
    # for k, v in rsp.headers.items():
    #   print(' ', f'{k}: {v}')
    for k in ('Date', 'Last-Modified'):
      v = rsp.headers.get(k)
      if v:
        print(' ', f'{k}: {v}')


if __name__ == '__main__':
  # check_date()
  main()

#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка контекстов цитирования
"""
from datetime import datetime
from functools import reduce
from typing import Callable, Iterable

from pymongo.database import Database
from pymongo import MongoClient
import requests

from loads.bundles import BUNDLES, update_bundles
from loads.classif_pos_neg import update_class_pos_neg
from loads.pubs import update_pubs_conts, SOURCE_XML
from loads.ngrams import update_ngramms, NGRAM_ROOT
from loads.topics import update_topics, TOPICS
from loads.common import AUTHORS

from utils import load_config_dev as load_config
# from utils import load_config_ord as load_config


def main():
  now = datetime.now
  start = now()
  print(start, 'start')
  for_del:int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  conf = load_config()
  conf_mongo = conf['mongodb']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb:Database = client[conf_mongo['db']]

    def do_upd(update:Callable, *args):
      print(now(), f'{update.__name__}: {update.__doc__}')
      ret = update(mdb, AUTHORS, for_del, *args)
      return ret

    colls = tuple(
      c for u, *args in (
        (update_pubs_conts, SOURCE_XML),
        (update_bundles, BUNDLES),
        (update_ngramms, NGRAM_ROOT),
        (update_topics, TOPICS),
        (update_class_pos_neg,),
      )
      for c in do_upd(u, *args)
    )

    for coll in colls:
      r = coll.delete_many({'for_del': for_del})
      print(now(), f'delete {coll.name}:', r.deleted_count)

  print(now(), 'end')


def check_date():
  for uri in flatten_uri(
    SOURCE_XML, BUNDLES, NGRAM_ROOT, TOPICS,
  ):
    for author in AUTHORS:
      uri_author = uri % dict(author=author)
      rsp = requests.head(uri_author)
      print(uri_author, rsp.status_code)
      # for k, v in rsp.headers.items():
      #   print(' ', f'{k}: {v}')
      for k in ('Date', 'Last-Modified'):
        v = rsp.headers.get(k)
        if v:
          print(' ', f'{k}: {v}')


def flatten_uri(*args):
  for elt in args:
    if isinstance(elt, str):
      if elt.startswith(('http://', 'https://')):
        yield elt
    elif isinstance(elt, Iterable):
      yield from flatten_uri(*elt)


if __name__ == '__main__':
  # check_date()
  main()

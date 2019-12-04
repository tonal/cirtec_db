#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка контекстов цитирования
"""
from collections import Counter
from datetime import datetime
from functools import reduce
import re

from pymongo.database import Database
from pymongo import MongoClient

from load_bundles import update_bundles, BUNDLES
from load_ngrams import update_ngramms, NGRAM_DIR
from load_pubs import update_pubs_conts, SOURCE_XML, SOURCE_URL
from utils import load_config


def main():
  start = datetime.now()
  for_del:int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  conf = load_config()['dev']
  conf_mongo = conf['mongodb']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb:Database = client[conf_mongo['db']]

    colls = (
      update_pubs_conts(mdb, for_del, SOURCE_URL, SOURCE_XML) +
      update_bundles(mdb, for_del, BUNDLES) +
      update_ngramms(mdb, for_del, NGRAM_DIR)
    )

    for coll in colls:
      r = coll.delete_many({'for_del': for_del})
      print(f'delete {coll.name}:', r.deleted_count)


if __name__ == '__main__':
  main()

#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка топиков и топиков в контексты
"""
from datetime import datetime
from functools import partial, reduce
import json
from typing import Iterable, Tuple
from urllib.request import urlopen

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from load_utils import AUTHORS
from utils import load_config


# BUNDLES:str = 'linked_papers_base.json'
BUNDLES:Tuple[str, ...] = (
  # 'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/linked_papers_base.json',
  'http://onir2.ranepa.ru:8081/prl/groups/%(author)s/linked_papers_base.json',
  'http://onir2.ranepa.ru:8081/prl/groups/%(author)s/cited_papers_base.json',
  'http://onir2.ranepa.ru:8081/prl/groups/%(author)s/citing_papers_base.json'
)


def main():
  now = datetime.now
  start = now()
  conf = load_config()
  conf_mongo = conf['dev']['mongodb']
  bundles:Tuple[str, ...] = BUNDLES
  for_del: int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  print(now(), 'start')

  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    update_bundles(mdb, AUTHORS, for_del, bundles)


def update_bundles(
  mdb:Database, authors:Iterable[str], for_del:int, bundles:Tuple[str, ...]
) -> Tuple[Collection, ...]:
  """Обновление коллекции bundles и дополнение в публикации и контексты"""

  cache_pubs = set()
  bund_in_cont = set()

  for author in authors:
    for bundle in bundles:
      update_bundles4load(mdb, bundle, author, cache_pubs, bund_in_cont)

  return ()


def update_bundles4load(
  mdb:Database, bundles:str, uni_author:str, cache_pubs, bund_in_cont
):

  now = datetime.now
  with urlopen(bundles % dict(author=uni_author)) as f:
    bundles = json.load(f)
  mbnds = mdb['bundles']
  mbnds_update = partial(mbnds.update_one, upsert=True)

  i = 0
  for i, (bundl_id, bundle) in enumerate(bundles.items(), 1):
    total_pubs = bundle.get('total pubs')
    total_cits = bundle.get('total cits')
    bundle_doc = dict(total_cits=total_cits, total_pubs=total_pubs)
    mbnds_update(dict(_id=bundl_id),
      {'$set': bundle_doc, '$unset': {'for_del': 1}})

    # all_intext_ref = bundle.get('all_intext_ref')
    # for iref in all_intext_ref or ():
    #   _, rpub_id, rstart = iref.rsplit('@', 2)
    #   if rpub_id not in cache_pubs:
    #     mpubs_update(dict(_id=rpub_id), {'$unset': {'for_del': 1}})
    #     cache_pubs.add(rpub_id)
    #   if (bundl_id, iref) not in bund_in_cont:
    #     bund_in_cont.add((bundle, iref))
    #     mcont_update(
    #       dict(_id=iref),
    #       {
    #         '$set': {'pub_id': rpub_id, 'start': fast_int(rstart)},
    #         '$addToSet': {'bundles': bundle}, '$unset': {'for_del': 1}})

    if i % 1000 == 0:
      print(now(), i, bundl_id)
  print(now(), i)
  return ()


if __name__ == '__main__':
  main()

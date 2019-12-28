#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка топиков и топиков в контексты
"""
from datetime import datetime
from functools import partial, reduce
import json
from typing import Tuple
from urllib.request import urlopen

from fastnumbers import fast_int
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from load_utils import rename_new_field
from utils import load_config, norm_spaces

# BUNDLES:str = 'linked_papers_base.json'
BUNDLES:str = 'http://onir2.ranepa.ru:8081/groups/authors/Sergey-Sinelnikov-Murylev/linked_papers_base.json'


def main():
  start = datetime.now()
  conf = load_config()
  conf_mongo = conf['dev']['mongodb']
  bundles:str = BUNDLES
  for_del: int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    mbnds, = update_bundles(mdb, for_del, bundles)

    r = mbnds.delete_many({'for_del': for_del})
    print(f'delete {mbnds.name}:', r.deleted_count)


def update_bundles(mdb:Database, for_del:int, bundles:str) -> Tuple[Collection]:
  """Обновление коллекции bundles и дополнение в публикации и контексты"""
  mbnds = mdb['bundles']
  mbnds_update = partial(mbnds.update_one, upsert=True)
  mcont = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)
  mpubs = mdb['publications']
  mpubs_insert = mpubs.insert_one
  mpubs_update = partial(mpubs.update_one, upsert=True)

  # topics = json.load(open(bundles, encoding='utf-8'))
  with urlopen(bundles) as f:
    topics = json.load(f)

  mbnds.update_many({}, {'$set': {'for_del': for_del}})

  def bib2bib(a, y, t):
    res = {}
    title = norm_spaces(t)
    if title:
      res.update(title=title)
    authors = a.strip()
    if authors:
      res.update(authors=authors.split())
    year = norm_spaces(y)
    if year:
      res.update(year=year)
    return res

  cache_pubs = set()
  cache_conts = set()
  i = 0
  for i, (pub_id, p_doc) in enumerate(topics.items(), 1):
    if pub_id in {'sum_cits', 'sum_pubs'}:
      break
    refs = p_doc.pop('refs')
    doc_refs = []
    for j, (ref_id, ref_cont) in enumerate(refs.items(), 1):
      num, rpub_id = ref_id.split('@', 1)
      assert pub_id == rpub_id
      assert j == fast_int(num)
      bib = ref_cont['bib']
      bundle = ref_cont.get('bundle')
      total_cits = ref_cont.get('total cits')
      total_pubs = ref_cont.get('total pubs')
      all_intext_ref = ref_cont.get('all_intext_ref')
      intext_ref = ref_cont.get('intext_ref')
      bib_bib = bib2bib(**bib)
      ref_doc = dict(num=j, **bib_bib)
      if bundle:
        ref_doc.update(bundle=bundle)
        bundle_doc = dict(**bib_bib)
        if total_cits:
          bundle_doc.update(total_cits=total_cits)
        if total_pubs:
          bundle_doc.update(total_pubs=total_pubs)
        mbnds_update(
          dict(_id=bundle),{
            '$set': bundle_doc, '$addToSet': {'bibs_new': bib_bib},
            '$unset': {'for_del': 1}})
      doc_refs.append(ref_doc)

      for iref in all_intext_ref or ():
        rpub_id, rstart = iref.rsplit('@', 1)
        if rpub_id not in cache_pubs:
          try:
            mpubs_insert(dict(_id=rpub_id), {})
          except DuplicateKeyError:
            pass
          cache_pubs.add(rpub_id)
        if not bundle and iref not in cache_conts:
          mcont_update(
            dict(_id=iref),
            {'$set': {'start': fast_int(rstart), '$unset': {'for_del': 1}}})
        elif bundle:
          mcont_update(
            dict(_id=iref), {
            '$set': {'pub_id': rpub_id, 'start': fast_int(rstart)},
            '$addToSet': {'bundles_new': bundle}, '$unset': {'for_del': 1}})
        cache_conts.add(iref)

    mpubs_update(
      dict(_id=pub_id), {'$set': dict(refs=doc_refs), '$unset': {'for_del': 1}})

    print(pub_id, len(doc_refs))

  rename_new_field(mbnds, 'bibs')
  rename_new_field(mcont, 'bundles')

  return (mbnds,)


if __name__ == '__main__':
  main()

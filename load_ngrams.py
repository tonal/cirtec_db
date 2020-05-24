#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка топиков и топиков в контексты
"""
from datetime import datetime
from functools import partial, reduce
import json
from typing import Iterable, Tuple
from urllib.parse import urljoin
from urllib.request import urlopen

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from load_utils import AUTHORS, rename_new_field
from utils import load_config


NGRAM_ROOT:str = 'http://onir2.ranepa.ru:8081/prl/data/%(author)s/'
NGRAM_DIR = (
  ('l', 'lemmas'), ('no-l', 'nolemmas'),)
NGRAM_TEMPL = '{nka}-gram-result.json'.format
NKAS = (2, 3, 4, 5, 6)


def main():
  start = datetime.now()
  conf = load_config()
  conf_mongo = conf['dev']['mongodb']
  for_del: int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    col_gramms, = update_ngramms(mdb, AUTHORS, for_del, NGRAM_ROOT)

    r = col_gramms.delete_many({'for_del': for_del})
    print(f'delete {col_gramms.name}:', r.deleted_count)


def update_ngramms(
  mdb:Database, authors:Iterable[str], for_del:int, ngramm_root:str
) -> Tuple[Collection]:
  """Обновление коллекции n_gramms и дополнение в контексты"""
  col_gramms = mdb['n_gramms']
  ngrm_update = partial(col_gramms.update_one, upsert=True)
  mcont = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)
  # mcont.update_many({}, {'$unset': {'linked_papers_ngrams': 1}})

  col_gramms.update_many({}, {'$set': {'for_del': for_del}})

  cash_cont = set()
  cnt = 0
  PREF = ('linked_papers_', 'cited_papers_', 'citing_papers_')
  # PREF_LEN = len(PREF)
  for uni_author in authors:
    ngramm_root_author = ngramm_root % dict(author=uni_author)
    for (ngramm_path, obj_type) in NGRAM_DIR:
      type_uri = urljoin(ngramm_root_author, f'{ngramm_path}/')
      print(type_uri)
      for nka in NKAS:
        # nka = int(fngram.stem.split('-', 1)[0])
        fname = NGRAM_TEMPL(nka=nka)
        ngramms_uri = urljoin(type_uri, fname)
        # col_name = f'gramm_{nka}_{NGRAM_DIR}'
        # print(fngram, nka, obj_type)
        print(ngramms_uri, nka, obj_type)
        # continue

        ngram_doc = None
        # ngrams = json.load(fngram.open(encoding='utf-8'))
        with urlopen(ngramms_uri) as f:
          ngrams = json.load(f)

        for title, doc in ngrams.items():
          all_cnt = doc['count']
          # print(' ', title, all_cnt)
          lp_cnt = 0

          ngr_id = f'{obj_type}_{title}'
          for k, cdoc in enumerate(doc['ids'], 1):
            g_pub_id, gcnt = cdoc.popitem()
            if not g_pub_id.startswith(PREF):
              continue
            # pub_id, ref_num, start = g_pub_id[PREF_LEN:].rsplit('_', 2)
            pg_cat, *pubparts, ref_num, start = g_pub_id.split('_')
            pub_id = '_'.join(pubparts)
            assert not pub_id.isdigit(), f'"{g_pub_id}" {pg_cat} {pub_id} {ref_num}, {start}'
            # print('  ', k, pub_id, ref_num, start, gcnt)
            cont_id = f'{pub_id}@{start}'
            if (cont_id, ngr_id) not in cash_cont:
              mcont_update(dict(_id=cont_id), {
                '$set': {'pub_id': pub_id, 'start': int(start)},
                '$addToSet': {
                  'ngrams_new': {'_id': ngr_id, 'cnt': gcnt},
                }})
              cash_cont.add((cont_id, ngr_id))
            lp_cnt += gcnt
          if not lp_cnt:
            continue
          ngram_doc = dict(_id=ngr_id, title=title, nka=nka, type=obj_type,
            count_all=all_cnt, count_in_linked_papers=lp_cnt, )
          ngrm_update(
            dict(_id=ngr_id), {'$set': ngram_doc, '$unset': {'for_del': 1}})
          cnt += 1
          if cnt % 1000 == 0:
            print(' ', cnt, ngram_doc)
            # return
        print(' ', cnt, ngram_doc)
  print(cnt)

  rename_new_field(mcont, 'ngrams')
  mcont.update_many(
    {"linked_papers_ngrams": {"$exists": 1}},
    {"$unset": {"linked_papers_ngrams": 1}})

  return (col_gramms,)


if __name__ == '__main__':
  main()

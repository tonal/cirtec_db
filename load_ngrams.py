#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка топиков и топиков в контексты
"""
from collections import Counter
from datetime import datetime
from functools import partial, reduce
import json
from operator import itemgetter
from pathlib import Path
from typing import Tuple, Sequence

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from utils import load_config


NGRAM_TEMPL = '*-gram-result.json'
NGRAM_DIR = [('l', 'lemmas')]


def main():
  start = datetime.now()
  conf = load_config()
  conf_mongo = conf['dev']['mongodb']
  for_del: int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    col_gramms, = update_ngramms(mdb, for_del, NGRAM_DIR)

    r = col_gramms.delete_many({'for_del': for_del})
    print(f'delete {col_gramms.name}:', r.deleted_count)


def update_ngramms(
  mdb:Database, for_del:int, ngramm_paths:Sequence[Tuple[str, str]]
) -> Tuple[Collection]:
  """Обновление коллекции n_gramms и дополнение в контексты"""
  print()
  col_gramms = mdb['n_gramms']
  col_gramms.update_many({}, {'$set': {'for_del': for_del}})
  ngrm_update = partial(col_gramms.update_one, upsert=True)
  mcont = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)
  mcont.update_many({}, {'$unset': {'linked_papers_ngrams': 1}})
  cnt = 0
  PREF = 'linked_papers_'
  PREF_LEN = len(PREF)
  for (ngramm_path, obj_type) in ngramm_paths:
    for fngram in Path(ngramm_path).glob(NGRAM_TEMPL):
      nka = int(fngram.stem.split('-', 1)[0])
      # col_name = f'gramm_{nka}_{NGRAM_DIR}'
      print(fngram, nka, obj_type)

      ngram_doc = None
      ngrams = json.load(fngram.open(encoding='utf-8'))
      for title, doc in ngrams.items():
        all_cnt = doc['count']
        # print(' ', title, all_cnt)
        lp_cnt = 0

        ngr_id = f'{obj_type}_{title}'
        for k, cdoc in enumerate(doc['ids'], 1):
          g_pub_id, gcnt = cdoc.popitem()
          if not g_pub_id.startswith(PREF):
            continue
          pub_id, ref_num, start = g_pub_id[PREF_LEN:].rsplit('_', 2)
          # print('  ', k, pub_id, ref_num, start, gcnt)
          cont_id = f'{pub_id}@{start}'
          mcont_update(dict(_id=cont_id), {
            '$set': {'pub_id': pub_id, 'start': start},
            '$addToSet': {
              'linked_papers_ngrams': {'_id': ngr_id, 'cnt': gcnt}}})
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
  return (col_gramms,)


if __name__ == '__main__':
  main()

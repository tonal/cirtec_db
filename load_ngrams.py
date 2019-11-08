#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка топиков и топиков в контексты
"""
from collections import Counter
from functools import partial
import json
from operator import itemgetter
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from utils import load_config


NGRAM_TEMPL = '*-gram-result.json'
NGRAM_DIR = 'l'


def main():
  conf = load_config()
  conf_mongo = conf['mongodb_dev']

  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    mdb.drop_collection('n_gramms')
    col_gramms = mdb['n_gramms']
    insert = col_gramms.insert_one
    mcont = mdb['contexts']
    mcont_update = partial(mcont.update_one, upsert=True)
    obj_type = 'lemmas'

    cnt = 0
    PREF = 'linked_papers_'
    PREF_LEN = len(PREF)
    for fngram in Path(NGRAM_DIR).glob(NGRAM_TEMPL):
      nka = int(fngram.stem.split('-', 1)[0])
      # col_name = f'gramm_{nka}_{NGRAM_DIR}'
      print(fngram, nka, obj_type)

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
          mcont_update(
            dict(_id=cont_id),
            {
              '$addToSet': {
                'linked_papers_ngrams': {'_id': ngr_id, 'cnt': gcnt}}})
          lp_cnt += gcnt
        if not lp_cnt:
          continue
        ngram_doc = dict(
          _id=ngr_id, title=title, nka=nka, type=obj_type, count_all=all_cnt,
          count_in_linked_papers=lp_cnt,)
        insert(ngram_doc)
        cnt += 1
        if cnt % 1000 == 0:
          print(' ', cnt, ngram_doc)
          # return
      print(' ', cnt, ngram_doc)
    print(cnt)


if __name__ == '__main__':
  main()

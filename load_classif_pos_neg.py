#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Расчёт и загрузка классификатора позитив/негатив для контекстов.
"""
from datetime import datetime
from functools import reduce
from operator import itemgetter

from joblib import load as jl_load
import pandas as pd
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression

from util_text import Text2Seq
from utils import load_config


def main():
  start = datetime.now()
  conf = load_config()
  conf_mongo = conf['dev']['mongodb']
  for_del: int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])

  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    r = update_class_pos_neg(mdb, for_del)



def update_class_pos_neg(mdb:Database, for_del:int):
  """Обновление данных о классах контекстов (positive, neutral, negative)
  """
  now = datetime.now
  start = now()
  mcont: Collection = mdb['contexts']
  mcont_update = mcont.update_one
  cvect: CountVectorizer = jl_load('positive_negative_cvect.joblib')
  print(now(), 'cvect')
  model: LogisticRegression = jl_load('positive_negative_model.joblib')
  print(now(), 'model')
  df_need = pd.DataFrame(data=parse_contexts(mcont), columns='cid seq'.split())
  df_need.info()
  # df_need.dropna(inplace=True)
  print(now(), df_need.head())
  X_need = cvect.transform(df_need.seq)
  df_need['labels'] = model.predict(X_need)
  # df_need.to_csv('res3_1.csv', columns='cid labels seq'.split(), index=False)
  i = 0
  for i, row in enumerate(df_need.itertuples(), 1):
    # print(row.cid)
    label = int(row.labels)
    cl = {
      -2: 'very negative', -1: 'negative', 0: 'neutral', 1: 'positive',
      2: 'very positive'}[label]
    pos_neg = {'val': label, 'class': cl}
    mcont_update(dict(_id=row.cid), {'$set': {'positive_negative': pos_neg}})
  # return
  end = now()
  print(end, i, end - start)
  return ()


def parse_contexts(mcont:Collection):
  wnorm = Text2Seq()
  seq2text = wnorm.seq2text
  get_flds = itemgetter('_id', 'prefix', 'exact', 'suffix')
  for i, cont in enumerate(mcont.find({'exact': {'$exists': True}}), 1):
    # print(i, f'"{li.rstrip()}"')
    cid, prefix, exact, suffix = get_flds(cont)
    text = ' '.join([prefix, exact, suffix])
    seq = seq2text(text)
    yield cid, seq


if __name__ == '__main__':
  main()

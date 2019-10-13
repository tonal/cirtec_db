#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Группа В. Показатели по повторяющимся фразам из контекстов цитирований
"""
from collections import Counter, defaultdict

from pymongo import MongoClient

from select4reportA import print_freq_ngramm_by_frag, get_topn

MONGO_URI = 'mongodb://localhost:27017/'


def main():
  client = MongoClient(MONGO_URI, compressors='snappy')
  mdb = client['cirtec']

  contexts = mdb.contexts
  print_freq_ngramm_by_frag(mdb, 50)
  print()
  print_top_ngramms_top_author_by_frags(mdb, 50)
  print()
  print_top_ngramms_topics_by_frags(mdb, 50)


def print_top_ngramms_top_author_by_frags(
  mdb, topn:int, *, topn_authors:int=500, nka:int=2, ltype:str='lemmas'
):
  """Кросс-распределение «фразы» - «со-цитирования»"""
  print('Кросс-распределение «фразы» - «со-цитирования»')

  n_gramms = mdb.n_gramms
  top_ngramms = get_topn(
    n_gramms, topn, preselect=[{'$match': {'nka': nka, 'type': ltype}}],
    sum_expr='$linked_papers.cnt')

  contexts = mdb.contexts

  for i, (ngramm, cnt, conts) in enumerate(top_ngramms, 1):
    frags = Counter()
    congr = defaultdict(Counter)

    for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$unwind': '$cocit_authors'},
      # {'$sort': {'frag_num': 1}},
    ]):
      ngr = doc['cocit_authors']

      fnum = doc['frag_num']
      frags[fnum] += 1
      congr[ngr][fnum] += 1

    msg = f'{"/".join(str(frags[i]) for i in range(1, 6))}'
    print(f"{i:<3d} '{ngramm}' {msg} ({cnt})") # sum(frags.values())

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1):
      msg = f'{"/".join(str(cnts[i]) for i in range(1, 6))}'
      print(f"   {j:<3d} '{co}': {msg} ({sum(cnts.values())})")


def print_top_ngramms_topics_by_frags(
  mdb, topn:int=10, *, nka:int=2, ltype:str='lemmas'
):
  """Кросс-распределение «фразы» - «топики контекстов цитирований»"""
  print('Кросс-распределение «фразы» - «топики контекстов цитирований»')

  n_gramms = mdb.n_gramms
  top_ngramms = get_topn(
    n_gramms, topn, preselect=[{'$match': {'nka': nka, 'type': ltype}}],
    sum_expr='$linked_papers.cnt')

  contexts = mdb.contexts

  for i, (ngrmm, cnt, conts) in enumerate(top_ngramms, 1):
    frags = Counter()
    congr = defaultdict(Counter)

    for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$lookup': {
        'from': 'topics', 'localField': '_id',
        'foreignField': 'linked_papers.cont_id', 'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.linked_papers'},
      {'$match': {'$expr': {'$eq': ["$_id", "$cont.linked_papers.cont_id"]}}},
      {'$project': {'cont.type': False}}, # 'cont.linked_papers': False,
    ]):
      cont = doc['cont']
      ngr = cont['title']
      fnum = doc['frag_num']
      congr[ngr][fnum] += 1

    msg = f'{"/".join(str(frags[i]) for i in range(1, 6))}'
    print(f"{i:<2d} '{ngrmm}' {msg} ({cnt})") # sum(frags.values())

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1):
      msg = f'{"/".join(str(cnts[i]) for i in range(1, 6))}'
      print(f"   {j:<2d} '{co}': {msg} ({sum(cnts.values())})")


if __name__ == '__main__':
  main()

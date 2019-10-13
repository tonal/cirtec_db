#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Группа Б. Показатели со-цитирований из контекстов цитирований
"""
from collections import Counter, defaultdict
from operator import itemgetter
from typing import Optional, Sequence, Union, Tuple

from pymongo import MongoClient

from select4reportA import (
  print_freq_cocitauth_by_frags, get_topn_cocit_authors, get_topn)

MONGO_URI = 'mongodb://localhost:27017/'


def main():
  client = MongoClient(MONGO_URI, compressors='snappy')
  mdb = client['cirtec']

  contexts = mdb.contexts
  print_freq_cocitauth_by_frags(contexts, 50)
  print()
  print_top_author_ngramms_by_frags(mdb, 10)
  print()
  print_top_author_topics_by_frags(mdb, 10)


def print_top_author_ngramms_by_frags(
  mdb, topn:int, *, topn_gramms:int=500, nka:int=2, ltype:str='lemmas'
):
  """Кросс-распределение «со-цитирования» - «фразы из контекстов цитирований»"""
  print('Кросс-распределение «со-цитирования» - «фразы из контекстов цитирований»')

  if topn_gramms:
    n_gramms = mdb.n_gramms
    top_ngramms = get_topn(
      n_gramms, topn_gramms, preselect=[{'$match': {'nka': nka, 'type': ltype}}],
      sum_expr='$linked_papers.cnt')
    exists = frozenset(t for t, _, _ in top_ngramms)

  contexts = mdb.contexts
  topN = get_topn_cocit_authors(contexts, topn, include_conts=True)

  for i, (cocitauthor, cnt, conts) in enumerate(topN, 1):
    frags = Counter()
    congr = defaultdict(Counter)

    for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}}, # 'cocit_authors': cocitauthor}}, #
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$lookup': {
        'from': 'n_gramms', 'localField': '_id',
        'foreignField': 'linked_papers.cont_id', 'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$match': {'cont.nka': nka, 'cont.type': ltype}},
      {'$unwind': '$cont.linked_papers'},
      {'$match': {'$expr': {'$eq': ["$_id", "$cont.linked_papers.cont_id"]}}},
      {'$project': {'cont.type': False}}, # 'cont.linked_papers': False,
      # {'$sort': {'frag_num': 1}},
    ]):
      cont = doc['cont']
      ngr = cont['title']
      if topn_gramms and ngr not in exists:
        continue

      fnum = doc['frag_num']
      frags[fnum] += 1
      congr[ngr][fnum] += cont['linked_papers']['cnt']

    msg = f'{"/".join(str(frags[i]) for i in range(1, 6))}'
    print(f"{i:<3d} '{cocitauthor}' {msg} ({cnt})") # sum(frags.values())

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1):
      msg = f'{"/".join(str(cnts[i]) for i in range(1, 6))}'
      print(f"   {j:<3d} '{co}': {msg} ({sum(cnts.values())})")


def print_top_author_topics_by_frags(mdb, topn:int):
  """Кросс-распределение «со-цитирования» - «топики контекстов цитирований»"""
  print('Кросс-распределение «со-цитирования» - «топики контекстов цитирований»')

  contexts = mdb.contexts
  topN = get_topn_cocit_authors(contexts, topn, include_conts=True)

  for i, (cocitauthor, cnt, conts) in enumerate(topN, 1):
    frags = Counter()
    congr = defaultdict(Counter)

    for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$lookup': {
        'from': 'topics', 'localField': '_id',
        'foreignField': 'linked_papers.cont_id', 'as': 'cont'}},
      {'$unwind': '$cont'},
      # {'$match': {'cont.nka': nka, 'cont.type': ltype}},
      {'$unwind': '$cont.linked_papers'},
      {'$match': {'$expr': {'$eq': ["$_id", "$cont.linked_papers.cont_id"]}}},
      {'$project': {'cont.type': False}}, # 'cont.linked_papers': False,
      # {'$sort': {'frag_num': 1}},
    ]):
      cont = doc['cont']
      ngr = cont['title']

      fnum = doc['frag_num']
      frags[fnum] += 1
      congr[ngr][fnum] += 1

    msg = f'{"/".join(str(frags[i]) for i in range(1, 6))}'
    print(f"{i:<3d} '{cocitauthor}' {msg} ({cnt})") # sum(frags.values())

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1):
      msg = f'{"/".join(str(cnts[i]) for i in range(1, 6))}'
      print(f"   {j:<3d} '{co}': {msg} ({sum(cnts.values())})")


if __name__ == '__main__':
  main()

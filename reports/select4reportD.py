#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Группа Г. Показатели по топикам для контекстов цитирований
"""
from collections import Counter, defaultdict

from pymongo import MongoClient

from reports.select4reportA import print_freq_topics_by_frags, get_topn
from utils import load_config


def main():
  conf = load_config()
  conf_mongo = conf['mongodb']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    contexts = mdb.contexts
    print_freq_topics_by_frags(mdb)
    print()
    print_topics_top_author_by_frags(mdb)
    print()
    print_topics_top_ngramm_by_frags(mdb, 10)


def print_topics_top_author_by_frags(mdb):
  """Кросс-распределение «топики» - «со-цитирования»"""
  print('Кросс-распределение «топики» - «со-цитирования»')

  top_topics = get_topn(mdb.topics, 100)

  contexts = mdb.contexts
  for i, (topic, cnt, conts) in enumerate(top_topics, 1):
    frags = Counter()
    congr = defaultdict(Counter)

    for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$unwind': '$cocit_authors'},
    ]):
      ngr = doc['cocit_authors']

      fnum = doc['frag_num']
      frags[fnum] += 1
      congr[ngr][fnum] += 1

    msg = f'{"/".join(str(frags[i]) for i in range(1, 6))}'
    print(f"{i:<3d} '{topic}' {msg} ({sum(frags.values())})") # cnt})") #

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1):
      msg = f'{"/".join(str(cnts[i]) for i in range(1, 6))}'
      print(f"   {j:<3d} '{co}': {msg} ({sum(cnts.values())})")


def print_topics_top_ngramm_by_frags(
  mdb, topn_gramms:int=10, nka:int=4, ltype:str='lemmas'
):
  """Кросс-распределение «топики» - «фразы»"""
  print('Кросс-распределение «топики» - «фразы»')

  top_topics = get_topn(mdb.topics, 100)

  contexts = mdb.contexts
  for i, (topic, cnt, conts) in enumerate(top_topics, 1):
    frags = Counter()
    congr = defaultdict(Counter)
    for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$lookup': {
        'from': 'n_gramms', 'localField': '_id',
        'foreignField': 'linked_papers.cont_id', 'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$match': {'cont.nka': nka, 'cont.type': ltype}},
      # {'$unwind': '$cont.linked_papers'},
      # {'$match': {'$expr': {'$eq': ["$_id", "$cont.linked_papers.cont_id"]}}},
      {'$project': {'cont.type': False}}, # 'cont.linked_papers': False,
      {'$sort': {'cont.count_in_linked_papers': -1, 'cont.count_all': -1}},
      # {'$limit': topn_gramms}
    ]):
      cont = doc['cont']
      ngr = cont['title']

      fnum = doc['frag_num']
      frags[fnum] += 1
      congr[ngr][fnum] += 1 # cont['linked_papers']['cnt']
      if len(congr) == topn_gramms:
        break

    msg = f'{"/".join(str(frags[i]) for i in range(1, 6))}'
    print(f"{i:<3d} '{topic}' {msg} ({sum(frags.values())})")

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1):
      msg = f'{"/".join(str(cnts[i]) for i in range(1, 6))}'
      print(f"   {j:<3d} '{co}': {msg} ({sum(cnts.values())})")



if __name__ == '__main__':
  main()

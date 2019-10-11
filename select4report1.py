#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""
from collections import Counter, defaultdict
from operator import itemgetter
from typing import Optional, Sequence

from pymongo import MongoClient


MONGO_URI = 'mongodb://localhost:27017/'


def main():
  client = MongoClient(MONGO_URI, compressors='snappy')
  mdb = client['cirtec']

  contexts = mdb.contexts
  # print_freq_contexts(contexts)
  # print()
  # print_freq_contexts_by_pubs(mdb)
  # print()
  # print_freq_cocitauth_by_frags(contexts, 50)
  # print()
  # print_freq_ngramm_by_frag(mdb)
  # print()
  print_freq_topics_by_frags(mdb)


def print_freq_contexts(contexts):
  """
  Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций
  """
  curs = contexts.aggregate([
    {'$match': {'frag_num': {'$gt': 0}}},
    {'$group': {'_id': '$frag_num', 'count': {'$sum': 1}}},
    {'$sort': {'_id': 1}}])
  cnts = Counter({doc['_id']: int(doc["count"]) for doc in curs})
  msg2 = f'{"/".join(str(cnts[i]) for i in range(1, 6))}'
  print(
    'Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций:',
    msg2)


def print_freq_contexts_by_pubs(mdb):
  """
  Распределение цитирований по 5-ти фрагментам для отдельных публикаций
  заданного автора.
  """
  publications = mdb.publications
  pubs = {
    pdoc['_id']: (pdoc['name'], Counter())
    for pdoc in publications.find({'name': {'$exists': True}})
  }
  contexts = mdb.contexts
  for doc in contexts.aggregate([
    {'$match': {'frag_num': {'$gt': 0}}},
    {'$group': {
      '_id': {'fn': '$frag_num', 'pub_id': '$pub_id'}, 'count': {'$sum': 1}}},
    {'$sort': {'_id': 1}}
  ]):

    did = doc['_id']
    pub_id = did['pub_id']
    frn = did['fn']
    pubs[pub_id][1][frn] += int(doc['count'])

  for i, (pid, pub) in enumerate(
    sorted(pubs.items(), key=lambda kv: sum(kv[1][1].values()), reverse=True),
    1
  ):
    pcnts = pub[1]
    msg2 = f'{"/".join(str(pcnts[i]) for i in range(1, 6))}'
    print(f'{i:<3d} {sum(pcnts.values()):3d} {msg2:<15s}' , pub[0])


def print_freq_cocitauth_by_frags(contexts, topn:int=5):
  """Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»"""
  top50 = tuple(
    (doc['_id'], doc['count']) for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, 'cocit_authors': {'$exists': True}}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$unwind': '$cocit_authors'},
      {'$group': {'_id': '$cocit_authors', 'count': {'$sum': 1}}},
      {'$sort': {'count': -1, '_id': 1}},
      {'$limit': topn}]))

  for i, (cocitauthor, cnt) in enumerate(top50, 1):
    frags = Counter()
    coaut = defaultdict(Counter)

    for doc in contexts.find(
      {'cocit_authors': cocitauthor, 'frag_num': {'$gt': 0}},
      projection=['frag_num', 'cocit_authors']
    ).sort('frag_num'):
      # print(i, doc)
      fnum = doc['frag_num']
      coauthors = frozenset(
        c for c in doc['cocit_authors'] if c != cocitauthor)
      frags[fnum] += 1
      for ca in coauthors:
        coaut[ca][fnum] += 1

    msg = f'{"/".join(str(frags[i]) for i in range(1, 6))}'
    print(f"{i:<2d} '{cocitauthor}' co-cited in fragments {msg} ({cnt})")  # , i)

    for j, (co, cnts) in enumerate(
      sorted(coaut.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1):
      # print(i, co, cnts)
      print(f"   {j:<2d} '{co}': "
            f"{', '.join('in fragment %s repeats: %s' % (f, c) for f, c in cnts.items())}")


def print_freq_ngramm_by_frag(mdb, topn:int=10, *, nka:int=2, ltype:str='lemmas'):
  """Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»"""
  n_gramms = mdb.n_gramms
  # get_as_tuple = itemgetter('_id', 'count', 'conts')
  # topN = tuple(
  #   get_as_tuple(doc) for doc in n_gramms.aggregate([
  #     {'$match': {'nka': nka, 'type': ltype}},
  #     {'$unwind': '$linked_papers'},
  #     {'$group': {
  #       '_id': '$title', 'count': {'$sum': '$linked_papers.cnt'}, # }},
  #       'conts': {'$addToSet': '$linked_papers.cont_id'}}},
  #     {'$sort': {'count': -1, '_id': 1}},
  #     {'$limit': topn}]))

  topN = get_topn(
    n_gramms, topn, preselect=[{'$match': {'nka': nka, 'type': ltype}}],
    sum_expr='$linked_papers.cnt')
  exists = frozenset(t for t, _, _ in topN)

  contexts = mdb.contexts
  for i, (ngrmm, cnt, conts) in enumerate(topN, 1):
    congr = defaultdict(Counter)

    for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
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
      if ngr not in exists:
        continue
      fnum = doc['frag_num']
      congr[ngr][fnum] += cont['linked_papers']['cnt']

    frags = congr.pop(ngrmm)
    msg = f'{"/".join(str(frags[i]) for i in range(1, 6))}'
    print(f"{i:<3d} '{ngrmm}' {msg} ({cnt})") # sum(frags.values())

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1):
      # print(i, co, cnts)
      msg = f'{"/".join(str(cnts[i]) for i in range(1, 6))}'
      print(f"   {j:<3d} '{co}': {msg} ({sum(cnts.values())})")


def get_topn(
  colls, topn:int=10, *, preselect:Optional[Sequence]=None, sum_expr=1
):
  get_as_tuple = itemgetter('_id', 'count', 'conts')
  if preselect:
    pipeline = list(preselect)
  else:
    pipeline = []
  pipeline += [
    {'$unwind': '$linked_papers'},
    {'$group': {
      '_id': '$title', 'count': {'$sum': sum_expr},
      'conts': {'$addToSet': '$linked_papers.cont_id'}}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$limit': topn}
  ]
  topN = tuple(get_as_tuple(doc) for doc in colls.aggregate(pipeline))

  return topN


def print_freq_topics_by_frags(mdb, topn:int=20):
  """Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»"""
  topics = mdb.topics
  topN = get_topn(topics, topn=topn)

  contexts = mdb.contexts
  for i, (ngrmm, cnt, conts) in enumerate(topN, 1):
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
      # {'$sort': {'frag_num': 1}},
    ]):
      cont = doc['cont']
      ngr = cont['title']
      # if ngr not in exists:
      #   continue
      fnum = doc['frag_num']
      congr[ngr][fnum] += 1

    frags = congr.pop(ngrmm)
    msg = f'{"/".join(str(frags[i]) for i in range(1, 6))}'
    print(f"{i:<2d} '{ngrmm}' {msg} ({cnt})") # sum(frags.values())

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1):
      # print(i, co, cnts)
      msg = f'{"/".join(str(cnts[i]) for i in range(1, 6))}'
      print(f"   {j:<2d} '{co}': {msg} ({sum(cnts.values())})")


if __name__ == '__main__':
  main()

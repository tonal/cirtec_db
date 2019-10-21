#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""
from collections import Counter, defaultdict
from operator import itemgetter
from typing import Optional, Sequence, Union, Tuple

from pymongo import MongoClient

from utils import load_config


def main():
  conf = load_config()
  conf_mongo = conf['mongodb']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    contexts = mdb.contexts
    print_freq_contexts(contexts)
    print()
    print_freq_contexts_by_pubs(mdb)
    print()
    print_freq_cocitauth_by_frags(contexts, 50)
    print()
    print_freq_ngramm_by_frag(mdb)
    print()
    print_freq_topics_by_frags(mdb)
    print()
    print_freqs_table(mdb)


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
  print('Распределение цитирований по 5-ти фрагментам')
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
  print('Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»')
  topN = get_topn_cocit_authors(contexts, topn)

  for i, (cocitauthor, cnt) in enumerate(topN, 1):
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


def get_topn_cocit_authors(contexts, topn, *, include_conts:bool=False):
  """Получить самых со-цитируемых авторов"""
  if include_conts:
    group = {
      '$group': {
        '_id': '$cocit_authors', 'count': {'$sum': 1},
        'conts': {'$addToSet': '$_id'}}}
    get_as_tuple = itemgetter('_id', 'count', 'conts')
  else:
    group = {'$group': {'_id': '$cocit_authors', 'count': {'$sum': 1}}}
    get_as_tuple = itemgetter('_id', 'count')
  top50 = tuple(
    get_as_tuple(doc) for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, 'cocit_authors': {'$exists': True}}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$unwind': '$cocit_authors'},
      # {'$group': {'_id': '$cocit_authors', 'count': {'$sum': 1}}},
      group,
      {'$sort': {'count': -1, '_id': 1}},
      {'$limit': topn},
  ]))
  return top50


def print_freq_ngramm_by_frag(mdb, topn:int=10, *, nka:int=2, ltype:str='lemmas'):
  """Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»"""
  print('Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»')
  n_gramms = mdb.n_gramms

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
  colls, topn:int=10, *, preselect:Optional[Sequence]=None,
  sum_expr:Union[int, str]=1
) -> Tuple:
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
  print('Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»')
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


def print_freqs_table(mdb):
  """Объединенная таблица где для каждого из 5 фрагментов"""
  print('Объединенная таблица где для каждого из 5 фрагментов')
  contexts = mdb.contexts
  curs = contexts.aggregate([
    {'$match': {'frag_num': {'$gt': 0}}},
    {'$group': {'_id': '$frag_num', 'count': {'$sum': 1}}},
    {'$sort': {'_id': 1}}])
  cnts = Counter({doc['_id']: int(doc["count"]) for doc in curs})

  get_as_tuple = itemgetter('_id', 'count', 'lst')

  curs = contexts.aggregate([
    {'$match': {'frag_num': {'$gt': 0}}},
    {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
    {'$unwind': '$cocit_authors'},
    {'$group': {
      '_id': '$frag_num', 'count': {'$sum': 1},
      'lst': {'$addToSet': '$cocit_authors'}}},
    # {'$sort': {'_id': 1}}
  ])
  cocit_auths = {
    fn: (cnt, tuple(sorted(cca)))
    for fn, cnt, cca in (get_as_tuple(doc) for doc in curs)
  }

  n_gramms = mdb.n_gramms

  curs = n_gramms.aggregate([
    {'$match': {'nka': 2, 'type': 'lemmas'}},
    {'$unwind': '$linked_papers'},
    {'$group': {
      '_id': '$title', 'count': {'$sum': '$linked_papers.cnt'},
      'cont_ids': {'$addToSet': '$linked_papers.cont_id'}}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$limit': 50},
    {'$lookup': {
      'from': 'contexts', 'localField': 'cont_ids', 'foreignField': '_id',
      'as': 'cont'}},
    {'$project': {'cont_ids': False}},
    {'$unwind': '$cont'},
    {'$group': {
      '_id': '$cont.frag_num', 'count': {'$sum': 1},
      'lst': {'$addToSet': '$_id'}}}
  ])
  ngramms = {
    fn: (cnt, tuple(ngr))
    for fn, cnt, ngr in (get_as_tuple(doc) for doc in curs)
  }

  curs = mdb.topics.aggregate([
    {'$unwind': '$linked_papers'},
    {'$group': {
      '_id': '$title', 'count': {'$sum': 1},
      'cont_ids': {'$addToSet': '$linked_papers.cont_id'}}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$lookup': {
      'from': 'contexts', 'localField': 'cont_ids', 'foreignField': '_id',
      'as': 'cont'}},
    {'$project': {'cont_ids': False}},
    {'$unwind': '$cont'},
    {'$group': {
        '_id': '$cont.frag_num', 'count': {'$sum': 1},
        'lst': {'$addToSet': '$_id'}}}
  ])
  topics = {
    fn: (cnt, tuple(ngr))
    for fn, cnt, ngr in (get_as_tuple(doc) for doc in curs)
  }

  for i in range(1, 6):
    print('Фрагмент:', i)
    print('  контекстов:', cnts[i])
    cca = cocit_auths.get(i)
    if cca:
      cnt, au = cca
      print(
        f'  со-цитируемых авторов: ({cnt})', ', '.join(f"'{a}'" for a in au))
    else:
      print('  со-цитируемых авторов: нет')
    print(
      f'  фразы: ({ngramms[i][0]})', ', '.join(f"'{n}'" for n in ngramms[i][1]))
    print(
      f'  топики: {topics[i][0]}', ', '.join(f"'{n}'" for n in topics[i][1]))



if __name__ == '__main__':
  main()

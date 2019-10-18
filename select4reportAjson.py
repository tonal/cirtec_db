#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""
from collections import Counter, defaultdict
import json
from operator import itemgetter
from typing import Optional, Sequence, Union, Tuple

from pymongo import MongoClient

from utuls import load_config


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
  """Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций"""
  print(
    'А',
    'Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций:',
    'freq_contexts.json')
  curs = contexts.aggregate([
    {'$match': {'frag_num': {'$gt': 0}}},
    {'$group': {'_id': '$frag_num', 'count': {'$sum': 1}}},
    {'$sort': {'_id': 1}}])
  cnts = Counter({doc['_id']: int(doc["count"]) for doc in curs})
  with open('out_json/freq_contexts.json', 'w') as out:
    json.dump(cnts, out, ensure_ascii=False)


def print_freq_contexts_by_pubs(mdb):
  """
  Распределение цитирований по 5-ти фрагментам для отдельных публикаций
  заданного автора.
  """
  print(
    'А', 'Распределение цитирований по 5-ти фрагментам:',
    'freq_contexts_by_pubs.json')
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

  out_pubs = {}
  for i, (pid, pub) in enumerate(
    sorted(pubs.items(), key=lambda kv: sum(kv[1][1].values()), reverse=True),
    1
  ):
    pcnts = pub[1]
    out_pubs[pid] = dict(descr=pub[0], sum=sum(pcnts.values()), frags=pcnts)

  with open('out_json/freq_contexts_by_pubs.json', 'w') as out:
    json.dump(out_pubs, out, ensure_ascii=False)


def print_freq_cocitauth_by_frags(contexts, topn:int=5):
  """Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»"""
  print(
    'А', 'Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»:',
    'freq_cocitauth_by_frags.json')
  topN = get_topn_cocit_authors(contexts, topn)

  out_dict = {}
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

    out_cocitauthors = {}
    out_dict[cocitauthor] = dict(
      sum=cnt, frags=frags, cocitauthors=out_cocitauthors)

    for j, (co, cnts) in enumerate(
      sorted(coaut.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      out_cocitauthors[co] = dict(frags=cnts, sum=sum(cnts.values()))

  with open('out_json/freq_cocitauth_by_frags.json', 'w') as out:
    json.dump(out_dict, out, ensure_ascii=False)


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
  print(
    'А',
    'Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»:',
    'freq_ngramm_by_frag.json')
  n_gramms = mdb.n_gramms

  topN = get_topn(
    n_gramms, topn, preselect=[{'$match': {'nka': nka, 'type': ltype}}],
    sum_expr='$linked_papers.cnt')
  exists = frozenset(t for t, _, _ in topN)

  out_dict = {}
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
    crossgrams = {}
    out_dict[ngrmm] = dict(sum=cnt, frags=frags, crossgrams=crossgrams)

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crossgrams[co] = dict(frags=cnts, sum=sum(cnts.values()))

  with open('out_json/freq_ngramm_by_frag.json', 'w') as out:
    json.dump(out_dict, out, ensure_ascii=False)


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
  print(
    'А',
    'Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»:',
    'freq_topics_by_frags.json')
  topics = mdb.topics
  topN = get_topn(topics, topn=topn)

  contexts = mdb.contexts
  out_dict = {}
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
    crosstopics = {}
    out_dict[ngrmm] = dict(sum=cnt, frags=frags, crosstopics=crosstopics)


    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crosstopics[co] = dict(frags=cnts, sum=sum(cnts.values()))

  with open('out_json/freq_topics_by_frags.json', 'w') as out:
    json.dump(out_dict, out, ensure_ascii=False)


def print_freqs_table(mdb):
  """Объединенная таблица где для каждого из 5 фрагментов"""
  print(
    'А', 'Объединенная таблица где для каждого из 5 фрагментов:',
    'freqs_table.json')
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

  out_dict = {}
  for i in range(1, 6):
    # print('Фрагмент:', i)
    # print('  контекстов:', cnts[i])
    out_frag = dict(contexts=cnts[i])
    cca = cocit_auths.get(i)
    if cca:
      cnt, au = cca
      out_frag.update(cocitauthors=dict(count=cnt, list=au))
    out_frag.update(
      ngramms=dict(count=ngramms[i][0], list=ngramms[i][1]),
      topics=dict(count=topics[i][0], list=topics[i][1]))

    out_dict[i] = out_frag

  with open('out_json/freqs_table.json', 'w') as out:
    json.dump(out_dict, out, ensure_ascii=False)


if __name__ == '__main__':
  main()

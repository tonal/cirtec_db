#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Группа В. Показатели по повторяющимся фразам из контекстов цитирований
"""
from collections import Counter, defaultdict
import json

from pymongo import MongoClient

from select4reportA import print_freq_ngramm_by_frag, get_topn
from utuls import load_config


def main():
  conf = load_config()
  conf_mongo = conf['mongodb']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'

    contexts = mdb.contexts
    # print_freq_ngramm_by_frag(mdb, 50)
    # print()
    print_top_ngramms_top_author_by_frags(mdb, 50)
    print()
    print_top_ngramms_topics_by_frags(mdb, 50)


def print_top_ngramms_top_author_by_frags(
  mdb, topn:int, *, topn_authors:int=500, nka:int=2, ltype:str='lemmas'
):
  """Кросс-распределение «фразы» - «со-цитирования»"""
  print(
    'В', 'Кросс-распределение «фразы» - «со-цитирования»:',
    'top_ngramms_top_author_by_frags.json')

  n_gramms = mdb.n_gramms
  top_ngramms = get_topn(
    n_gramms, topn, preselect=[{'$match': {'nka': nka, 'type': ltype}}],
    sum_expr='$linked_papers.cnt')

  contexts = mdb.contexts

  out_dict = {}
  for i, (ngramm, cnt, conts) in enumerate(top_ngramms, 1):
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

    crosscocitaith = {}
    out_dict[ngramm] = dict(
      sum=cnt, frags=frags, cocitaithors=crosscocitaith)

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crosscocitaith[co] = dict(frags=cnts, sum=sum(cnts.values()))

  with open('out_json/top_ngramms_top_author_by_frags.json', 'w') as out:
    json.dump(out_dict, out, ensure_ascii=False)


def print_top_ngramms_topics_by_frags(
  mdb, topn:int=10, *, nka:int=2, ltype:str='lemmas'
):
  """Кросс-распределение «фразы» - «топики контекстов цитирований»"""
  print(
    'В', 'Кросс-распределение «фразы» - «топики контекстов цитирований»:',
    'top_ngramms_topics_by_frags.json')

  n_gramms = mdb.n_gramms
  top_ngramms = get_topn(
    n_gramms, topn, preselect=[{'$match': {'nka': nka, 'type': ltype}}],
    sum_expr='$linked_papers.cnt')

  contexts = mdb.contexts

  out_dict = {}
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
      frags[fnum] += 1
      congr[ngr][fnum] += 1

    crosstopics = {}
    out_dict[ngrmm] = dict(sum=cnt, frags=frags, crosstopics=crosstopics)

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crosstopics[co] = dict(frags=cnts, sum=sum(cnts.values()))

  with open('out_json/top_ngramms_topics_by_frags.json', 'w') as out:
    json.dump(out_dict, out, ensure_ascii=False)


if __name__ == '__main__':
  main()

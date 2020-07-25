#! /usr/bin/env python3
# -*- codong: utf-8 -*-
from typing import List, Optional

from models_dev.common import (
  _add_topic2pipeline, filter_by_pubs_acc, get_ngramm_filter)
from models_dev.models import AuthorParam, Authors, NgrammParam


def get_top_cocitauthors(
  topn:Optional[int], authorParams: AuthorParam
):
  pipeline = [
    {'$match': {'frag_num': {'$gt': 0}, 'cocit_authors': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'bundles': 0, 'ngrams': 0, 'topics': 0}},]

  pipeline += filter_by_pubs_acc(authorParams)

  pipeline += [
    {'$unwind': '$cocit_authors'},
    {'$group': {
      '_id': '$cocit_authors', 'count': {'$sum': 1},
      'conts': {'$addToSet': '$_id'}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_top_cocitauthors_publications(
  topn: Optional[int], authorParams: AuthorParam
):
  pipeline = [
    {'$match': {'cocit_authors': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'ngrams': 0,
      "topics": 0}}, ]
  pipeline += filter_by_pubs_acc(authorParams)

  pipeline += [
    {'$unwind': '$cocit_authors'},
    {'$group': {
      '_id': '$cocit_authors', 'count': {'$sum': 1},
      'pubs': {'$addToSet': '$pubid'}, }},
    {'$sort': {'count': -1, '_id': 1}}]
  if topn:
    pipeline += [{'$limit': topn}]

  pipeline += [{
    '$project': {
      "name": "$_id", "_id": 0, "count": {"$size": "$pubs"}, "pubs": "$pubs", }}]
  return pipeline


def get_top_cocitrefs(
  topn:Optional[int], authorParams: AuthorParam
):
  pipeline = [
    {'$match': {'frag_num': {'$gt': 0}, 'bundles': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'ngrams': 0, 'topics': 0}},]

  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {'$unwind': '$bundles'},
    {'$group': {
        '_id': '$bundles', 'count': {'$sum': 1},
        'conts': {'$addToSet': '$_id'}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  pipeline += [
    {'$lookup': {
      'from': 'bundles', 'localField': '_id', 'foreignField': '_id',
      'as': 'bundles'}},
    {'$unwind': '$bundles'},
  ]
  return pipeline


def get_top_cocitrefs2(
  topn: Optional[int], authorParams: AuthorParam
):
  pipeline = [
    {'$match': {'bundles': {'$exists': 1}, 'frag_num': {'$exists': 1}}},
    {'$project': {'pubid': 1, 'bundles': 1, 'frag_num': 1}},]

  if filter := filter_by_pubs_acc(authorParams):
    pipeline += filter

  pipeline += [
    {'$unwind': '$bundles'},
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}},
    {'$project': {
      'pubid': 1, 'bundles': 1, 'frag_num': 1, 'cont.bundles': 1}},
    {'$unwind': '$cont'},
    {'$unwind': '$cont.bundles'},
    {'$match': {'$expr': {'$ne': ['$bundles', '$cont.bundles']}}},
    {'$group': {
      '_id': {
        'cocitref1': {
          '$cond': [{'$gte': ['$bundles', '$cont.bundles'], },
            '$cont.bundles', '$bundles']},
        'cocitref2': {
          '$cond': [{'$gte': ['$bundles', '$cont.bundles'], },
            '$bundles', '$cont.bundles']},
        'cont_id': '$_id'},
      'pubid': {'$first': '$pubid'}, 'frag_num': {'$first': '$frag_num'},}},
    {'$sort': {'_id': 1}},
    {'$group': {
      '_id': {
        'cocitref1': '$_id.cocitref1', 'cocitref2': '$_id.cocitref2'},
      'count': {'$sum': 1},
      "pubs": {"$addToSet": "$pubid"},
      "frags": {"$push": "$frag_num"},
      'conts': {"$addToSet": "$_id.cont_id"},}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$project': {
      'cocitpair': ['$_id.cocitref1', '$_id.cocitref2'], '_id': 0,
      'intxt_cnt': '$count', 'pub_cnt': {"$size": "$pubs"}, "frags": "$frags",
      "pubids": "$pubs", "intxtids": "$conts"}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  return pipeline


def get_top_ngramms(
  topn:Optional[int], authorParams: AuthorParam, ngrammParam: NgrammParam
):
  pipeline = [
    {'$match': {'ngrams._id': {'$exists': 1}}},
    {'$project': {'prefix': 0, 'suffix': 0, 'exact': 0, 'topics': 0}},
  ]
  pipeline += filter_by_pubs_acc(authorParams)

  pipeline += [
    {'$unwind': '$ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]

  if nf := get_ngramm_filter(ngrammParam, 'ngrm'):
    pipeline += [nf]

  pipeline += [
  {'$group': {
      '_id': {'title': '$ngrm.title', 'type': '$ngrm.type'},
      'count': {'$sum': '$ngrams.cnt'},
      'count_cont': {'$sum': 1},
      'conts': {
        '$push': {'cont_id': '$_id', 'cnt': '$ngrams.cnt'}}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]

  if topn:
    pipeline += [{'$limit': topn}]

  return pipeline


def get_top_ngramm_author_stat(
  topn:Optional[int], author:Authors, np:NgrammParam
) -> List[dict]:
  assert author
  assert not np.is_empty()
  aname = author.value
  pipeline = [
    {'$match': {
      '$or': [
        {'uni_authors': aname,}, {'uni_cited': aname}, {'uni_citing': aname}]}},
    {'$addFields': {
      'atypes': {'$setUnion': [
        {'$cond': {
          'if': {'$in': [aname, {'$ifNull': ['$uni_authors', []]}]},
          'then': ['author'], 'else': []}},
        {'$cond': {
          'if': {'$in': [aname, {'$ifNull': ['$uni_cited', []]}]},
          'then': ['cited'], 'else': []}},
        {'$cond': {
          'if': {'$in': [aname, {'$ifNull': ['$uni_citing', []]}]},
          'then': ['citing'], 'else': []}},
        ]}}},
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
      'as': 'cont'}},
    {'$unwind': '$cont'},
    {'$unwind': '$cont.ngrams'},
    {'$match': {'cont.ngrams.nka': np.nka, 'cont.ngrams.type': np.ltype.value}},
    {'$group': {
      '_id': '$cont.ngrams._id',
      'atypes': {'$push': {
        'atype': '$atypes', 'cnt_tot': '$cont.ngrams.cnt',
        'contid': '$cont._id'}},
      'cnt': {'$sum': 1}, 'cnt_tot': {'$sum': '$cont.ngrams.cnt'}}},
    {'$sort': {'cnt': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  len_drop = len(np.ltype.value) + 1
  pipeline += [
    {'$project': {
      '_id': 0,
      'ngrm': {
        '$substrCP': [
          '$_id', len_drop, {'$subtract': [{'$strLenCP': "$_id" }, len_drop]}]},
      'cnt': '$cnt', 'cnt_tot': '$cnt_tot', 'atypes': '$atypes'
      }},
  ]
  return pipeline


def get_top_ngramms_publications(
  topn: Optional[int], authorParams: AuthorParam, ngrammParam: NgrammParam
):
  pipeline = [
    {'$match': {"ngrams": {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'cocit_authors': 0,
      "topics": 0}}, ]
  pipeline += filter_by_pubs_acc(authorParams)

  pipeline += [
    {'$unwind': '$ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},]
  if nf := get_ngramm_filter(ngrammParam, 'ngrm'):
    pipeline += [nf]
  pipeline += [
    {'$group': {
      '_id': '$ngrams._id', 'count': {'$sum': '$ngrams.cnt'},
      "ngrm": {"$first": "$ngrm"}, "pubs": {'$addToSet': '$pubid'}, }},
    {'$sort': {'count': -1, '_id': 1}}]
  if topn:
    pipeline += [{'$limit': topn}]

  pipeline += [
    {'$project': {
      "title": "$ngrm.title", "type": "$ngrm.type", "nka": "$ngrm.nka",
      "_id": 0, "count": "$count", "pubs": "$pubs", }}]
  return pipeline


def get_top_topics(
  topn:Optional[int], authorParams: AuthorParam, probability:Optional[float]
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'ngrams': 0}},
  ]
  pipeline += filter_by_pubs_acc(authorParams)

  pipeline += [
    {'$unwind': '$topics'},
  ]
  if probability :
    pipeline += [
      {'$match': {'topics.probability': {'$gte': probability}}},]

  pipeline += _add_topic2pipeline(authorParams)

  pipeline += [
    {'$group': {
      '_id': '$topic.title', 'count': {'$sum': 1},
      'probability_avg': {'$avg': '$topics.probability'},
      'probability_stddev': {'$stdDevPop': '$topics.probability'},
      'conts': {'$addToSet': '$_id'}, }},
    {'$sort': {'count': -1, '_id': 1}}
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  pipeline += [
    {'$project': {
      "topic": "$_id", "_id": 0, "count": "$count",
      "probability_avg": {"$round": ["$probability_avg", 2]},
      "probability_stddev": {"$round": ["$probability_stddev", 2]},
      "contects": "$conts",}}
  ]
  return pipeline


def get_top_topics_publications(
  topn: Optional[int], authorParams: AuthorParam, probability:Optional[float]
):
  pipeline = [
    {'$match': {"topics": {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'cocit_authors': 0,
      "ngrams": 0}}, ]
  pipeline += filter_by_pubs_acc(authorParams)

  pipeline += [{'$unwind': '$topics'},]

  if probability:
    pipeline += [
      {'$match': {"topics.probability": {'$gte': probability}}},]

  pipeline += _add_topic2pipeline(authorParams)
  pipeline += [
    {'$group': {
      '_id': '$topics.title', 'count': {'$sum': 1},
      'probability_avg': {'$avg': '$topics.probability'},
      'probability_stddev': {'$stdDevPop': '$topics.probability'},
      "pubs": {'$addToSet': '$pubid'}, }},
    {'$sort': {'count': -1, '_id': 1}}]
  if topn:
    pipeline += [{'$limit': topn}]

  pipeline += [
    {'$project': {
      "topic": "$_id", "_id": 0, "count_pubs": {"$size": "$pubs"},
      "count_conts": "$count",
      "probability_avg": {"$round": ["$probability_avg", 2]},
      "probability_stddev": {"$round": ["$probability_stddev", 2]},
      "pubs": "$pubs", }}]
  return pipeline

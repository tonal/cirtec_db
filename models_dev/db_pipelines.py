# -*- codong: utf-8 -*-
import logging
from typing import List, Optional

from models_dev.common import (
  filter_by_pubs_acc, filter_acc_dict, get_ngramm_filter, _add_topic2pipeline)
from models_dev.models import AuthorParam, NgrammParam

_logger = logging.getLogger('cirtec')


def get_refbindles(topn:Optional[int], authorParams: AuthorParam):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'topics': 0,
      'ngrams': 0}},
  ]

  pipeline += filter_by_pubs_acc(authorParams)

  pipeline += [
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},  ##
    {'$group': {
      '_id': '$bundles', 'cits': {'$sum': 1}, 'pubs': {'$addToSet': '$pubid'},
      'pos_neg': {'$push': '$positive_negative'},
      'frags': {'$push': '$frag_num'}, }},
    {'$project': {
      'cits': 1, 'pubs': {'$size': '$pubs'}, "pubs_ids": '$pubs', 'pos_neg': 1,
      'frags': 1}},
    {'$lookup': {
      'from': 'bundles', 'localField': '_id', 'foreignField': '_id',
      'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$project': {
      'cits': 1, 'pubs': 1, 'pubs_ids': 1,
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title', 'pos_neg': 1, 'frags': 1, }},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}}, # {$count: 'cnt'}
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  # _logger.info('pipeline: %s': )
  return pipeline


def get_refauthors(topn: Optional[int], authorParams: AuthorParam):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'topics': 0,
      'ngrams': 0}},
    {'$unwind': '$bundles'},
  ]

  pipeline += filter_by_pubs_acc(authorParams)

  pipeline += [
    # {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
      'as': 'bun'}},
    {'$unwind': '$bun'},
    {'$unwind': '$bun.authors'},
    {'$group': {
      '_id': '$bun.authors', 'cits': {'$addToSet': '$_id'},
      'cits_all': {'$sum': 1}, 'pubs': {'$addToSet': '$pubid'},
      'bunds_ids': {'$addToSet': '$bundles'},
      'bunds': {
        '$addToSet': {
          '_id': '$bun._id', 'total_cits': '$bun.total_cits',
          'total_pubs': '$bun.total_pubs'}},
      'pos_neg': {'$push': '$positive_negative'},
      'frags': {'$push': '$frag_num'}}},
    {'$project': {
      '_id': 0, 'author': '$_id', 'cits': {'$size': '$cits'},
      'cits_all': '$cits_all', 'bunds_cnt': {'$size': '$bunds_ids'},
      'pubs': {'$size': '$pubs'}, 'total_cits': {'$sum': '$bunds.total_cits'},
      'total_pubs': {'$sum': '$bunds.total_pubs'}, 'pos_neg': 1, 'frags': 1}},
    {'$sort': {'cits_all': -1, 'cits': -1, 'pubs': -1, 'author': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frag_publications(authorParams: AuthorParam):
  pipeline = [
    {'$match': {'name': {'$exists': 1}}},
  ]

  if filter := filter_acc_dict(authorParams):
    pipeline += [{'$match': filter},]

  pipeline += [
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
      'as': 'cont'}},
    {'$unwind': {'path': '$cont', 'preserveNullAndEmptyArrays': True}},
    {'$group': {
      '_id': {'pubid': '$_id', 'fn': '$cont.frag_num'}, 'count': {'$sum': 1},
      'title': {'$first': '$name'}}},
    {'$sort': {'_id': 1}},
    {'$group': {
      '_id': '$_id.pubid',
      'frags': {'$push': {'fn': '$_id.fn', 'count': '$count'}},
      'sum': {'$sum': '$count'}, 'descr': {'$first': '$title'}}},
    {'$sort': {'sum': -1, '_id': 1}},
  ]
  return pipeline


def get_refauthors_part(topn:int, authorParams: AuthorParam):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'topics': 0,
      'ngrams': 0}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
      'as': 'bun'}},
    {'$unwind': '$bun'},
    {'$unwind': '$bun.authors'},
    {'$group': {
      '_id': '$bun.authors', 'cits': {'$addToSet': '$_id'},
      'cits_all': {'$sum': 1}, 'pubs': {'$addToSet': '$pubid'},
      'bunds_ids': {'$addToSet': '$bundles'},
      'bunds': {
        '$addToSet': {
          '_id': '$bun._id', 'total_cits': '$bun.total_cits',
          'total_pubs': '$bun.total_pubs'}},
      'pos_neg': {'$push': '$positive_negative'},
      'frags': {'$push': '$frag_num'}}},
    {'$project': {
      '_id': 0, 'author': '$_id', 'cits': {'$size': '$cits'},
      'cits_all': '$cits_all', 'bunds_cnt': {'$size': '$bunds_ids'},
      'pubs': {'$size': '$pubs'}, 'total_cits': {'$sum': '$bunds.total_cits'},
      'total_pubs': {'$sum': '$bunds.total_pubs'}, 'pos_neg': 1, 'frags': 1}},
    {'$sort': {'cits_all': -1, 'cits': -1, 'pubs': -1, 'author': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_cocitauthors(
  topn:Optional[int], authParams: AuthorParam
) -> List[dict]:
  pipeline = [
    {'$match': {'frag_num': {'$gt': 0}, 'cocit_authors': {'$exists': True}}},
    {'$project': {'prefix': False, 'suffix': False, 'exact': False}},]
  pipeline += filter_by_pubs_acc(authParams)
  pipeline += [
    {'$unwind': '$cocit_authors'},
    {'$group': {
        '_id': '$cocit_authors', 'count': {'$sum': 1},
        'frags': {'$push': '$frag_num'}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn},]
  return pipeline


def get_frags_cocitauthors_cocitauthors(
  topn:Optional[int], authorParams: AuthorParam
):
  pipeline = [
    {'$match': {
      'cocit_authors': {'$exists': 1}, 'frag_num': {'$exists': 1},}},
    {'$project': {'pubid': 1, 'cocit_authors': 1, 'frag_num': 1}},]
  if filter_pipiline := filter_by_pubs_acc(authorParams):
    pipeline += filter_pipiline
    pipeline += [{'$project': {'pub': 0}},]
  pipeline += [
    {"$unwind": "$cocit_authors"},
    {"$lookup": {
      "from": "contexts", "localField": "_id", "foreignField": "_id",
      "as": "cont"}},
    {"$project": {
      "pubid": 1,"cocit_authors": 1,"frag_num": 1,"cont.cocit_authors": 1}},
    {"$unwind": "$cont"},
    {"$unwind": "$cont.cocit_authors"},
    {"$match": {"$expr": {"$lt": ["$cocit_authors", "$cont.cocit_authors"]}}},
    {"$project": {
      "_id": {
        "author1": "$cocit_authors", "author2": "$cont.cocit_authors",
        "cont_id": "$_id"},
      "cont": {"pubid": "$pubid", "frag_num": "$frag_num"}}},
    {"$sort": {"_id": 1}},
    {"$group": {
      "_id": {"author1": "$_id.author1","author2": "$_id.author2"},
      "count": {"$sum": 1},
      "conts": {
        "$push": {
          "cont_id": "$_id.cont_id", "pubid": "$cont.pubid",
          "frag_num": "$cont.frag_num"}}}},
    {"$sort": {"count": -1, "_id": 1}},
    {"$project": {
        "_id": 1,
        "cocitpair": {"author1": "$_id.author1", "author2": "$_id.author2"},
        "count": "$count", "conts": "$conts"}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_cocitauthors_ngramms(
  topn: Optional[int], authorParams: AuthorParam, ngrammParam: NgrammParam
):
  pipeline = [
    {"$match": {
      "cocit_authors": {"$exists": 1}, "frag_num": {"$exists": 1},
      "ngrams": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "cocit_authors": 1, "frag_num": 1,
      "ngrams": 1}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {"$unwind": "$cocit_authors"},
    {"$unwind": "$ngrams"},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},]
  if not ngrammParam.is_empty():
    pipeline += [get_ngramm_filter(ngrammParam, 'ngrm')]
  pipeline += [
    {"$group": {
      "_id": {
        "cocit_authors": "$cocit_authors", "ngram": "$ngrams._id",
        "cont_id": "$_id"},
      "cont": {"$first": {"pubid": "$pubid", "frag_num": "$frag_num"}},
      'count': {'$sum': "$ngrams.cnt"},
      'ngrm': {'$first': "$ngrm"},}},
    {"$sort": {"_id": 1}},
    {"$group": {
      "_id": {
        "cocit_authors": "$_id.cocit_authors", "ngram": "$_id.ngram"},
      "count": {"$sum": "$count"},
      "frags": {'$push': {"fn": "$cont.frag_num", "cnt": "$count"}},
      "conts": {
        "$push": {
          "cont_id": "$_id.cont_id", "pubid": "$cont.pubid",
          "frag_num": "$cont.frag_num"}},
      'ngrm': {'$first': "$ngrm"},}},
    {'$group': {
        "_id": "$_id.cocit_authors",
        "count": {"$sum": "$count"},
        "ngrms": {
          "$push": {"ngrm": "$ngrm", "count": "$count", "frags": "$frags"}},
        "conts2": {"$push": "$conts"},}},
    {"$project": {
        "count": 1, "ngrms": 1,
        "conts": {
            "$reduce": {
               "input": "$conts2", "initialValue": [],
            "in": {"$setUnion": ["$$value", "$$this"]}}}}},
    {"$sort": {"count": -1, "_id": 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_cocitauthors_topics(
  topn:Optional[int], authorParams: AuthorParam, probability:Optional[float]
):
  pipeline = [
    {"$match": {
      "cocit_authors": {"$exists": 1}, "frag_num": {"$exists": 1},
      "topics": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "cocit_authors": 1, "frag_num": 1,
      "topics": 1}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {"$unwind": "$cocit_authors"},
    {"$unwind": "$topics"},]
  if probability:
    pipeline += [
      {"$match": {"topics.probability": {"$gte": probability}}, }]

  pipeline = _add_topic2pipeline(authorParams)

  pipeline += [
    {"$group": {
      "_id": {
        "cocit_authors": "$cocit_authors", "topic": "$topic.title",
        "cont_id": "$_id"},
      "cont": {"$first": {"pubid": "$pubid", "frag_num": "$frag_num"}},}},
    {"$sort": {"_id": 1}},
    {"$group": {
      "_id": {
        "cocit_authors": "$_id.cocit_authors", "topic": "$_id.topic"},
      "count": {"$sum": 1},
      "frags": {'$push': {"fn": "$cont.frag_num", "cnt": "$count"}},
      "conts": {
        "$push": {
          "cont_id": "$_id.cont_id", "pubid": "$cont.pubid",
          "frag_num": "$cont.frag_num"}},}},
    {'$group': {
        "_id": "$_id.cocit_authors",
        "count": {"$sum": "$count"},
        "topics": {
          "$push": {"topic": "$_id.topic", "count": "$count", "frags": "$frags"}},
        "conts2": {"$push": "$conts"},}},
    {"$project": {
        "count": 1, "topics": 1,
        "conts": {
            "$reduce": {
               "input": "$conts2", "initialValue": [],
            "in": {"$setUnion": ["$$value", "$$this"]}}}}},
    {"$sort": {"count": -1, "_id": 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_ngramms(
  topn:Optional[int], authorParams: AuthorParam, ngrammParam: NgrammParam
):
  pipeline = [
    {'$match': {
      'frag_num': {'$exists': 1}, 'ngrams': {'$exists': 1}}},
    {'$project': {
      'pubid': 1, 'frag_num': 1, 'linked_paper': '$ngrams'}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {'$unwind': '$linked_paper'},
    {'$group': {
      '_id': {'_id': '$linked_paper._id', 'frag_num': '$frag_num'},
      'count': {'$sum': '$linked_paper.cnt'},}},
    {'$group': {
      '_id': '$_id._id', 'count': {'$sum': '$count'},
      'frags': {'$push': {'frag_num': '$_id.frag_num', 'count': '$count',}},}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$lookup': {
      'from': 'n_gramms', 'localField': '_id', 'foreignField': '_id',
      'as': 'ngramm'}},
    {'$unwind': '$ngramm'},
  ]

  if not ngrammParam.is_empty():
    pipeline += [get_ngramm_filter(ngrammParam, 'ngramm')]

  pipeline += [
    {'$project': {
      'title': '$ngramm.title', 'type': '$ngramm.type', 'nka': '$ngramm.nka',
      'count': '$count', 'frags': '$frags'}}]

  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_ngramms_cocitauthors(
  topn: Optional[int], authorParams: AuthorParam, ngrammParam: NgrammParam
):
  pipeline = [
    {"$match": {
      "cocit_authors": {"$exists": 1}, "frag_num": {"$exists": 1},
      "ngrams": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "cocit_authors": 1, "frag_num": 1,
      "ngrams": 1}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {"$unwind": "$cocit_authors"},
    {"$unwind": "$ngrams"},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},]
  if not ngrammParam.is_empty():
    pipeline += [get_ngramm_filter(ngrammParam, 'ngrm')]
  pipeline += [
    {"$group": {
      "_id": {
        "ngram": "$ngrams._id", "cocit_authors": "$cocit_authors",
        "cont_id": "$_id"},
      "cont": {"$first": {"pubid": "$pubid", "frag_num": "$frag_num"}},
      'count': {'$sum': "$ngrams.cnt"},
      'ngrm': {'$first': "$ngrm"},}},
    {"$sort": {"_id": 1}},
    {"$group": {
      "_id": {
        "ngram": "$_id.ngram", "cocit_authors": "$_id.cocit_authors"},
      "count": {"$sum": "$count"},
      "frags": {'$push': {"fn": "$cont.frag_num", "cnt": "$count"}},
      'ngrm': {'$first': "$ngrm"},}},
    {'$group': {
      "_id": "$_id.ngram",
      "title": {"$first": "$ngrm.title"},
      "type": {"$first": "$ngrm.type"},
      "nka": {"$first": "$ngrm.nka"},
      "count": {"$sum": "$count"},
      "auths": {
        "$push": {
          "auth": "$_id.cocit_authors", "count": "$count", "frags": "$frags"}},}},
    {"$project": {"_id": 0,}},
    {"$sort": {"count": -1, "title": 1, 'type': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_ngramms_ngramms_branch_root(
  topn:Optional[int], authorParams: AuthorParam, ngrammParam: NgrammParam
):
  pipeline = [
    {'$match': {'ngrams': {'$exists': 1}, 'frag_num': {'$gt': 0}}},]

  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {'$project': {'linked_paper': '$ngrams'}},
    {'$unwind': '$linked_paper'},
    {'$group': {
      '_id': '$linked_paper._id', 'count': {'$sum': '$linked_paper.cnt'},
      'cont_ids': {'$addToSet': '$_id'},}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$lookup': {
      'from': 'n_gramms', 'localField': '_id', 'foreignField': '_id',
      'as': 'ngramm'}},
    {'$unwind': '$ngramm'},
  ]

  if nf := get_ngramm_filter(ngrammParam, 'ngramm'):
    pipeline += [nf]

  pipeline += [
    {'$project': {
      'title': '$ngramm.title', 'type': '$ngramm.type', "nka": "$ngramm.nka",
      'count': 1, 'conts': '$cont_ids'}}
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_ngramms_ngramms_branch(ngrammParam: NgrammParam):
  pipeline = [
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'topics': 0, 'bundles': 0}},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'cont'}},
    {'$unwind': '$cont'},
  ]

  if nf := get_ngramm_filter(ngrammParam, 'cont'):
    pipeline += [nf]

  pipeline += [
    {'$unwind': '$ngrams'},
    {'$match': {'$expr': {'$eq': ['$ngrams._id', '$cont._id']}}},
  ]
  return pipeline


def get_frags_ngramms_topics(
  topn: Optional[int], authorParams: AuthorParam, ngrammParam: NgrammParam,
  probability: Optional[float]
):
  pipeline = [
    {"$match": {
      "topics": {"$exists": 1}, "frag_num": {"$exists": 1},
      "ngrams": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "frag_num": 1, "topics": 1,
      "ngrams": 1}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {"$unwind": "$ngrams"},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]
  if nf := get_ngramm_filter(ngrammParam, 'ngrm'):
    pipeline += [nf]

  pipeline += [
    {"$unwind": "$topics"},
  ]
  if probability:
    pipeline += [
      {'$match': {'topics.probability': {'$gte': probability}}},
    ]
  pipeline += [
    {"$group": {
      "_id": {
        "ngram": "$ngrams._id",
        "topic": "$topics._id",
        "cont_id": "$_id"},
      "cont": {"$first": {"pubid": "$pubid", "frag_num": "$frag_num"}},
      'count': {'$sum': "$ngrams.cnt"},
      'ngrm': {'$first': "$ngrm"},}},
    {"$sort": {"_id": 1}},
    {"$group": {
      "_id": {
        "ngram": "$_id.ngram", "topic": "$_id.topic"},
      "count": {"$sum": "$count"},
      "frags": {'$push': {"fn": "$cont.frag_num", "cnt": "$count"}},
      'ngrm': {'$first': "$ngrm"},}},]

  pipeline += _add_topic2pipeline(authorParams, localField='_id.topic')

  pipeline += [
    {'$group': {
      "_id": "$_id.ngram",
      "title": {"$first": "$ngrm.title"},
      "type": {"$first": "$ngrm.type"},
      "nka": {"$first": "$ngrm.nka"},
      "count": {"$sum": "$count"},
      "topics": {
        "$push": {
          "topic": "$topic.title", "count": "$count", "frags": "$frags"}},}},
    {"$project": {"_id": 0,}},
    {"$sort": {"count": -1, "title": 1, 'type': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frag_pos_neg_cocitauthors2(
  topn: Optional[int], authorParams: AuthorParam
):
  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': 1}, 'cocit_authors': {'$exists': 1}}},
    {'$project': {
      'pubid': 1, 'positive_negative': 1, 'cocit_authors': 1, 'frag_num': 1}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {'$project': {'pub': 0}},
    {'$unwind': '$cocit_authors'},
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}},
    {'$project': {
      'pubid': 1, 'positive_negative': 1, 'cocit_authors': 1, 'frag_num': 1,
      'cont.cocit_authors': 1}},
    {'$unwind': '$cont'},
    {'$unwind': '$cont.cocit_authors'},
    {"$match": {"$expr": {"$lt": ["$cocit_authors", "$cont.cocit_authors"]}}},
    {"$project": {
      "_id": {
        "author1": "$cocit_authors", "author2": "$cont.cocit_authors",
        "cont_id": "$_id"},
      "cont": {
        "pubid": "$pubid", "frag_num": "$frag_num",
        'positive_negative': '$positive_negative'}}},
    {'$sort': {'_id': 1}},
    {'$group': {
      '_id': {
        'author1': '$_id.author1',
        'author2': '$_id.author2'},
      'count': {'$sum': 1},
      'conts': {'$push': {
        'cont_id': '$_id.cont_id', 'pubid': '$cont.pubid',
        'frag_num': '$cont.frag_num',
        'positive_negative': '$cont.positive_negative',},},}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$project': {
      '_id': 0,
      'cocitpair': {
        'author1': '$_id.author1', 'author2': '$_id.author2'},
      'count': '$count', 'conts': '$conts', }},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frag_pos_neg_contexts(authorParams: AuthorParam):
  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': 1}, 'frag_num': {'$exists': 1}}},
    {'$project': {'pubid': 1, 'positive_negative': 1, 'frag_num': 1}},
  ]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {'$group': {
      '_id': {
        'class_pos_neg': {
        '$arrayElemAt': [
          ['neutral', 'positive', 'positive', 'negative', 'negative'],
          '$positive_negative.val']},
        'frag_num': '$frag_num'},
      'pubids': {'$addToSet': '$pubid'},
      'intxtids': {'$addToSet': '$_id'}}},
    {'$sort': {'_id.class_pos_neg': -1}},
    {'$group': {
      '_id': '$_id.frag_num',
      'classes': {'$push': {
          'pos_neg': '$_id.class_pos_neg',
          'pubids': '$pubids', 'intxtids': '$intxtids'}}}},
    {'$project': {'_id': 0, 'frag_num': '$_id', 'classes': '$classes'}},
    {'$sort': {'frag_num': 1}},
  ]
  return pipeline


def get_frags_topics(
  topn:Optional[int], authorParams: AuthorParam, probability:Optional[float]
):
  pipeline = [
    {'$match': {
      'frag_num': {'$exists': 1}, 'topics': {'$exists': 1}}},
    {'$project': {'pubid': 1, 'frag_num': 1, 'topics': 1}},]
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
      '_id': {'_id': '$topic.title', 'frag_num': '$frag_num'},
      'count': {'$sum': 1,}}},
    {'$group': {
      '_id': '$_id._id', 'count': {'$sum': '$count'},
      'frags': {'$push': {'frag_num': '$_id.frag_num', 'count': '$count',}},}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_topics_cocitauthors(
  authorParams: AuthorParam, probability:Optional[float]
):
  pipeline = [
    {"$match": {
      "cocit_authors": {"$exists": 1}, "frag_num": {"$exists": 1},
      "topics": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "cocit_authors": 1, "frag_num": 1,
      "topics": 1}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {"$unwind": "$cocit_authors"},
    {"$unwind": "$topics"},]
  if probability:
    pipeline += [
      {"$match": {"topics.probability": {"$gte": probability}}, }]

  pipeline += _add_topic2pipeline(authorParams)
  pipeline += [
    {"$group": {
      "_id": {
        "cocit_authors": "$cocit_authors", "topic": "$topic.title",
        "cont_id": "$_id"},
      "cont": {"$first": {"pubid": "$pubid", "frag_num": "$frag_num"}},}},
    {"$sort": {"_id": 1}},
    {"$group": {
      "_id": {
        "cocit_authors": "$_id.cocit_authors", "topic": "$_id.topic"},
      "count": {"$sum": 1},
      "frags": {'$push': {"fn": "$cont.frag_num", "cnt": "$count"}},
      "conts": {
        "$push": {
          "cont_id": "$_id.cont_id", "pubid": "$cont.pubid",
          "frag_num": "$cont.frag_num"}},}},
    {'$group': {
        "_id": "$_id.topic",
        "count": {"$sum": "$count"},
        "auths": {
          "$push": {
            "auth": "$_id.cocit_authors", "count": "$count", "frags": "$frags"}},
        "conts2": {"$push": "$conts"},}},
    {"$project": {
        "count": 1, "auths": 1,
        "conts": {
            "$reduce": {
               "input": "$conts2", "initialValue": [],
            "in": {"$setUnion": ["$$value", "$$this"]}}}}},
    {"$sort": {"count": -1, "_id": 1}},
  ]
  return pipeline


def get_frags_topics_ngramms(
  authorParams: AuthorParam, ngrammParam: NgrammParam,
  probability: Optional[float], topn_crpssgramm:Optional[int]
):
  pipeline = [
    {"$match": {
      "topics": {"$exists": 1}, "frag_num": {"$exists": 1},
      "ngrams": {'$exists': 1}}},
    {"$project": {"pubid": 1, "topics": 1, "frag_num": 1, "ngrams": 1}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {"$unwind": "$ngrams"},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]
  if nf := get_ngramm_filter(ngrammParam, 'ngrm'):
    pipeline += [nf]
  pipeline += [
    {"$unwind": "$topics"},
  ]
  if probability:
    pipeline += [
      {'$match': {'topics.probability': {'$gte': probability}}},
    ]
  pipeline += _add_topic2pipeline(authorParams)
  pipeline += [
    {"$group": {
      "_id": {
        "topic": "$topic.title",
        "ngram": "$ngrams._id",
        "cont_id": "$_id"},
      "cont": {"$first": {"pubid": "$pubid", "frag_num": "$frag_num"}},
      'count': {'$sum': "$ngrams.cnt"},
      'ngrm': {'$first': "$ngrm"},}},
    {"$sort": {"_id": 1}},
    {"$group": {
      "_id": {"topic": "$_id.topic", "ngram": "$_id.ngram"},
      "count": {"$sum": "$count"},
      "frags": {'$push': {"fn": "$cont.frag_num", "cnt": "$count"}},
      'ngrm': {'$first': "$ngrm"},}},
    {"$sort": {"count": -1, "_id": 1}},
    {'$group': {
      "_id": "$_id.topic",
      "count": {"$sum": "$count"},
      "crossgrams": {
        "$push": {
          "title": "$ngrm.title", "type": "$ngrm.type", "nka": "$ngrm.nka",
          "count": "$count", "frags": "$frags"}},}},
    {"$project": {"_id": 0, "topic": "$_id", "count": 1, "crossgrams": 1}},]

  if topn_crpssgramm:
    pipeline += [
      {"$project": {
        "topic": 1, "count": 1,
        "crossgrams": {"$slice": ["$crossgrams", topn_crpssgramm]}}}, ]
  pipeline += [
    {"$sort": {"count": -1, "title": 1}},
  ]
  return pipeline


def get_frags_topics_topics(
  authorParams: AuthorParam, probability: Optional[float]
):
  pipeline = [
    {"$match": {
      "frag_num": {"$exists": 1}, "topics": {'$exists': 1}}},
    {"$project": {"pubid": 1, "frag_num": 1, "topics": 1}},
  ]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {"$project": {"pub": 0}},
    {"$unwind": "$topics"},]
  if probability:
    pipeline += [
      {"$match": {
        "topics.probability": {"$gte": probability}}, }]
  pipeline += _add_topic2pipeline(authorParams)
  pipeline += [
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}},
    {'$unwind': "$cont"},
    {'$unwind': "$cont.topics"},]
  if probability:
    pipeline += [
      {"$match": {
        "cont.topics.probability": {"$gte": probability}}, }]
  pipeline += _add_topic2pipeline(
    authorParams, localField='cont.topics._id', as_field='cont_topic')
  pipeline += [
    {'$project': {
      "frag_num": 1, "topic1": "$topic.title",
      "topic2": "$cont_topic.title"}},
    {'$match': {'$expr': {'$ne': ["$topic1", "$topic2"]}}},
    {'$group': {
      "_id": {
        "topic1": {
          '$cond': [{'$gte': ["$topic1", "$topic2"]}, "$topic1", "$topic2"]},
        "topic2": {
          '$cond': [{'$gte': ["$topic1", "$topic2"]}, "$topic2", "$topic1"]},
        "cont_id": "$_id"}, "frag_num": {"$first": "$frag_num"}}},
    {"$sort": {"_id": 1}},
    {"$group": {
      "_id": {"topic1": "$_id.topic1", "topic2": "$_id.topic2"},
      "count": {"$sum": 1},
      "frags": {"$push": "$frag_num"}}},
    {"$sort": {"count": -1, "_id": 1}},
    {'$group': {
      '_id': "$_id.topic1", "count": {"$sum": "$count"},
      'crosstopics': {
        '$push': {
          "topic": "$_id.topic2", "frags": "$frags", "count": "$count"}}}},
    {"$sort": {"count": -1, "_id": 1}},
  ]
  return pipeline


def get_pos_neg_cocitauthors(
  topn:Optional[int], authorParams: AuthorParam
):
  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': True},
      'cocit_authors': {'$exists': True}}},
    {'$project': {
      'pubid': True, 'positive_negative': True, 'cocit_authors': True}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {'$unwind': '$cocit_authors'},
    {'$group': {
      '_id': {
        'pos_neg': {
          '$arrayElemAt': [
            ['neutral', 'positive', 'positive', 'negative', 'negative'],
            '$positive_negative.val']}, 'title': '$cocit_authors'},
      'coauthor_cnt': {'$sum': 1}}},
    {'$sort': {'coauthor_cnt': -1, '_id.title': 1}},
    {'$group': {
      '_id': '$_id.pos_neg', 'cocitauthor': {
        '$push': {'author': '$_id.title', 'count': '$coauthor_cnt'}}, }},]
  if topn:
    pipeline += [{
      '$project': {
        '_id': False, 'class_pos_neg': '$_id',
        'cocitauthors': {'$slice': ['$cocitauthor', topn]}}},]
  else:
    pipeline += [{
      '$project': {
        '_id': False, 'class_pos_neg': '$_id', 'cocitauthors': '$cocitauthor'}}]

  pipeline += [
    {'$sort': {'class_pos_neg': -1}}, ]
  return pipeline


def get_pos_neg_contexts(authorParams: AuthorParam):
  pipeline = [
    {'$match': {'positive_negative': {'$exists': True}}},
    {'$project': {'pubid': True, 'positive_negative': True}},]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {'$group': {
      '_id': {
        '$arrayElemAt': [
          ['neutral', 'positive', 'positive', 'negative', 'negative'],
          '$positive_negative.val']},
      'pubids': {'$addToSet': '$pubid'},
      'contids': {'$addToSet': '$_id'}}},
    {'$project': {
      '_id': False, 'class_pos_neg': '$_id',
      'cont_cnt': {'$size': '$contids'}, 'pub_cnt': {'$size': '$pubids'},
      'pubids': '$pubids', 'contids': '$contids'}},
    {'$sort': {'class_pos_neg': -1}},
  ]
  return pipeline


def get_pos_neg_ngramms(
  topn: Optional[int], authorParams: AuthorParam, ngrammParam: NgrammParam
):
  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': True},
      'ngrams': {'$exists': True},}},
    {'$project': {
      'pubid': True, 'positive_negative': True, 'ngrams': True}},]
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
      '_id': {
        'pos_neg': {
          '$arrayElemAt': [
            ['neutral', 'positive', 'positive', 'negative', 'negative'],
            '$positive_negative.val']},
        'title': '$ngrm.title', 'nka': '$ngrm.nka', 'ltype': '$ngrm.type'},
      'ngrm_cnt': {'$sum': '$ngrams.cnt'}}},
    {'$sort': {'ngrm_cnt': -1, '_id.title': 1}},
    {'$group': {
      '_id': '$_id.pos_neg',
      'ngramms': {
        '$push': {
          'title': '$_id.title', 'nka': '$_id.nka', 'ltype': '$_id.ltype',
          'count': '$ngrm_cnt'}},}},]
  if topn:
    pipeline += [{
      '$project': {
        '_id': False, 'class_pos_neg': '$_id',
        'ngramms': {'$slice': ['$ngramms', topn]}, }},]
  else:
    pipeline += [{
      '$project': {
        '_id': False, 'class_pos_neg': '$_id', 'ngramms': '$ngramms' }},]

  pipeline += [
    {'$sort': {'class_pos_neg': -1}},
  ]
  return pipeline


def get_pos_neg_pubs(authorParams: AuthorParam):
  pipeline = [
    {'$match': {'positive_negative': {'$exists': 1}, }},]
  if filter_pipeline := filter_by_pubs_acc(authorParams):
    pipeline += filter_pipeline
  pipeline += [
    {'$group': {
      '_id': {'pid': '$pubid', 'pos_neg': '$positive_negative.val'},
      'cnt': {'$sum': 1}}},
    {'$group': {
      '_id': '$_id.pid',
      'pos_neg': {'$push': {'val': '$_id.pos_neg', 'cnt': '$cnt'}}}},
    {'$sort': {'_id': 1}},
    {'$lookup': {
        'from': 'publications', 'localField': '_id', 'foreignField': '_id',
        'as': 'pub'}}, {'$unwind': '$pub'},
      {'$unwind': '$pub'},
    ]

  pipeline += [
    {'$project': {'pos_neg': True, 'pub.name': True}}]
  return pipeline


def get_pos_neg_topics(
  authorParams: AuthorParam, probability:Optional[float]
):
  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': 1},
      'topics': {'$exists': 1}}},
    {'$project': {
      'pubid': 1, 'positive_negative': 1, 'topics': 1}},]
  if filter_pipeline := filter_by_pubs_acc(authorParams):
    pipeline += filter_pipeline

  pipeline += [
    {'$unwind': '$topics'},
    {'$match': {'topics.probability': {'$gte': probability}}},]
  pipeline += _add_topic2pipeline(authorParams)
  pipeline += [
    {'$group': {
      '_id': {
        'pos_neg': {
          '$arrayElemAt': [
            ['neutral', 'positive', 'positive', 'negative', 'negative'],
            '$positive_negative.val']},
        'topic': '$topic.title'},
      'topic_cnt': {'$sum': 1}}},
    {'$sort': {'topic_cnt': -1}},
    {'$group': {
      '_id': '$_id.pos_neg',
      'topics': {'$push': {'topic': '$_id.topic', 'count': '$topic_cnt'}},}},
    {'$project': {
      '_id': 1, 'class_pos_neg': '$_id',
      'topic_cnt': {'$size': '$topics'}, 'topics': '$topics'}},
    {'$sort': {'class_pos_neg': -1}},
  ]
  return pipeline


def get_publications_cocitauthors(
  authorParams: AuthorParam, topn_auth:Optional[int]
):
  pipeline = [
    {"$match": {"cocit_authors": {"$exists": 1}}}]
  if filter_pipeline := filter_by_pubs_acc(authorParams):
    pipeline += filter_pipeline
  else:
    pipeline += [
      {'$lookup': {
        'from': 'publications', 'localField': 'pubid', 'foreignField': '_id',
        'as': 'pub'}},
      {'$unwind': '$pub'},]

  pipeline += [
    {'$project': {'prefix': 0, 'suffix': 0, 'exact': 0}},
    {'$unwind': '$cocit_authors'},
    {'$group': {
        '_id': {"pubid": "$pubid", "cocit_authors": '$cocit_authors'},
        'count': {'$sum': 1}, "name": {"$first": "$pub.name"},
        'conts': {'$addToSet': '$_id'}}},
    {'$sort': {'count': -1, '_id': 1}},
    {"$group": {
      "_id": "$_id.pubid",
      "count": {'$sum': "$count"}, "name": {"$first": "$name"},
      "cocitauthors": {
        "$push": {"cocit_authors": "$_id.cocit_authors", "conts": "$conts"}}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn_auth:
    pipeline += [
      {"$project": {
        "count": 1, "name": 1,
        "cocitauthors": {"$slice": ["$cocitauthors", topn_auth]}}}
    ]

  pipeline += [
    {"$project": {
      "_id": 0, "pubid": "$_id", "count": 1, "name": 1, "cocitauthors": 1,
      "conts": {
        "$reduce": {
          "input": "$cocitauthors", "initialValue": [],
          "in": {"$setUnion": ["$$value", "$$this.conts"]}}}
    }},
  ]
  return pipeline


def get_publications_ngramms(
  topn:Optional[int], authorParams: AuthorParam, ngrammParam: NgrammParam,
  topn_gramm:Optional[int]
):
  pipeline = [
    {"$match": {"ngrams": {"$exists": 1}}}]
  if filter_pipeline := filter_by_pubs_acc(authorParams):
    pipeline += filter_pipeline
  else:
    pipeline += [
      {'$lookup': {
        'from': 'publications', 'localField': 'pubid', 'foreignField': '_id',
        'as': 'pub'}},
      {'$unwind': '$pub'},
    ]

  pipeline += [
    {'$unwind': '$ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'}, ]
  if nf := get_ngramm_filter(ngrammParam, 'ngrm'):
    pipeline += [nf]

  pipeline += [
    {'$group': {
        '_id': {"pubid": "$pubid", "ngrm": '$ngrams._id'},
        'count': {'$sum': 1}, "name": {"$first": "$pub.name"},
        'conts': {'$addToSet': '$_id'}}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$lookup': {
      'from': 'n_gramms', 'localField': '_id.ngrm', 'foreignField': '_id',
      'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
    {"$group": {
      "_id": "$_id.pubid",
      "count": {'$sum': "$count"}, "name": {"$first": "$name"},
      "crossgrams": {
        "$push": {
          "title": "$ngrm.title", "type": "$ngrm.type", "nka": "$ngrm.nka",
          "conts": "$conts",}}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]

  if topn:
    pipeline += [{"$limit": topn}]

  if topn_gramm:
    pipeline += [
      {"$project": {
        "pubid": "$_id", "_id": 0, "count": "$count", "name": "$name",
        "conts": {
          "$reduce": {
            "input": "$crossgrams", "initialValue": [],
            "in": {"$setUnion": ["$$value", "$$this.conts"]}}},
        "crossgrams": {"$slice": ["$crossgrams", topn_gramm]},
      }},]
  else:
    pipeline += [
      {"$project": {
        "pubid": "$_id", "_id": 0, "count": "$count", "name": "$name",
        "conts": {
          "$reduce": {
            "input": "$crossgrams", "initialValue": [],
            "in": {"$setUnion": ["$$value", "$$this.conts"]}}},
        "crossgrams": "$crossgrams",
      }},]
  return pipeline


def get_publications_topics_topics(
  authorParams: AuthorParam, probability:Optional[float]
):
  pipeline = [
    {"$match": {
      "pubid": {"$exists": 1}, "topics": {'$exists': 1}}},
    {"$project": {"pubid": 1, "topics": 1}}, ]
  pipeline += filter_by_pubs_acc(authorParams)
  pipeline += [
    {"$project": {"pub": 0}},
    {"$unwind": "$topics"}, ]
  if probability:
    pipeline += [{
      "$match": {
        "topics.probability": {"$gte": probability}}, }]
  pipeline += _add_topic2pipeline(authorParams)
  pipeline += [{
    '$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}}, {'$unwind': "$cont"},
    {'$unwind': "$cont.topics"}, ]
  if probability:
    pipeline += [{
      "$match": {
        "cont.topics.probability": {"$gte": probability}}, }]
  pipeline += _add_topic2pipeline(
    authorParams, localField='cont.topics._id', as_field='cont_topic')
  pipeline += [{
    '$project': {
      "pubid": 1, "topic1": "$topic.title",
      "topic2": "$cont_topic.title"}},
    {'$match': {'$expr': {'$ne': ["$topic1", "$topic2"]}}}, {
      '$group': {
        "_id": {
          "topic1": {
            '$cond': [{'$gte': ["$topic1", "$topic2"]}, "$topic1", "$topic2"]},
          "topic2": {
            '$cond': [{'$gte': ["$topic1", "$topic2"]}, "$topic2", "$topic1"]},
          "cont_id": "$_id"},
        "pubid": {"$first": "$pubid"}}},
    {"$sort": {"_id": 1}}, {
      "$group": {
        "_id": {"topic1": "$_id.topic1", "topic2": "$_id.topic2"},
        "count": {"$sum": 1}, "pubid": {"$push": "$pubid"}}},
    {"$sort": {"count": -1, "_id": 1}},
    {'$group': {
      '_id': "$_id.topic1", "count": {"$sum": "$count"},
      'crosstopics': {
        '$push': {
          "topic": "$_id.topic2", "pubs": "$pubid", "count": "$count"}}}},
    {"$sort": {"count": -1, "_id": 1}},
    {"$project": {
      "topic": "$_id", "_id": 0, "count": 1, "crosstopics": 1,
      "pubs": {
        "$reduce": {
          "input": "$crosstopics", "initialValue": [],
          "in": {"$setUnion": ["$$value", "$$this.pubs"]}}}
    }}
  ]
  return pipeline

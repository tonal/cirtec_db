# -*- codong: utf-8 -*-
import enum
import logging
from typing import Optional


_logger = logging.getLogger('cirtec')


class LType(enum.Enum):
  lemmas = 'lemmas'
  nolemmas = 'nolemmas'


def get_refbindles_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str]
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'topics': 0,
      'ngrams': 0}},
  ]

  pipeline += filter_by_pubs_acc(author, cited, citing)

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


def filter_by_pubs_acc(
  author:Optional[str], cited:Optional[str], citing:Optional[str],
):
  if not any([author, cited, citing]):
    return []
  match = filter_acc_dict(author, cited, citing)
  pipeline = [
    {'$lookup': {
      'from': 'publications', 'localField': 'pubid', 'foreignField': '_id',
      'as': 'pub'}},
    {'$unwind': '$pub'},
    # {'$match': {'pub.uni_authors': {'$exists': 1}}},
    {'$match': {f'pub.{key}': val for key, val in match.items()}},
  ]
  return pipeline


def filter_acc_dict(
  author:Optional[str], cited:Optional[str], citing:Optional[str],
):
  """Фильтр по author, cited, citing"""
  if not any([author, cited, citing]):
    return {}
  match = {
    f'uni_{key}': val for key, val in
      (('authors', author), ('cited', cited), ('citing', citing)) if val}
  return match


def get_refauthors_pipeline(
  topn: Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str]
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'topics': 0,
      'ngrams': 0}},
    {'$unwind': '$bundles'},
  ]

  pipeline += filter_by_pubs_acc(author, cited, citing)

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


def get_frag_publications(
  author: Optional[str], cited: Optional[str], citing: Optional[str]
):
  pipeline = [
    {'$match': {'name': {'$exists': 1}}},
  ]

  if filter := filter_acc_dict(author, cited, citing):
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


def get_top_cocitauthors_publications_pipeline(
  topn: Optional[int], author: Optional[str], cited: Optional[str],
  citing: Optional[str]
):
  pipeline = [
    {'$match': {'cocit_authors': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'ngrams': 0,
      "topics": 0}}, ]
  pipeline += filter_by_pubs_acc(author, cited, citing)

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


def get_top_cocitrefs2_pipeline(
  topn: Optional[int], author: Optional[str], cited: Optional[str],
  citing: Optional[str]
):
  pipeline = [
    {'$match': {'bundles': {'$exists': 1}, 'frag_num': {'$exists': 1}}},
    {'$project': {'pubid': 1, 'bundles': 1, 'frag_num': 1}},]

  if filter := filter_by_pubs_acc(author, cited, citing):
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


def get_top_ngramms_publications_pipeline(
  topn: Optional[int], author: Optional[str], cited: Optional[str],
  citing: Optional[str], nka:Optional[int], ltype:Optional[LType]
):
  pipeline = [
    {'$match': {"ngrams": {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'cocit_authors': 0,
      "topics": 0}}, ]
  pipeline += filter_by_pubs_acc(author, cited, citing)

  pipeline += [
    {'$unwind': '$ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},]
  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]
  pipeline += [
    {'$group': {
      '_id': '$ngrams', 'count': {'$sum': 1},
      "ngrm": {"$first": "$ngrm"}, "pubs": {'$addToSet': '$pubid'}, }},
    {'$sort': {'count': -1, '_id': 1}}]
  if topn:
    pipeline += [{'$limit': topn}]

  pipeline += [
    {'$project': {
      "title": "$ngrm.title", "type": "$ngrm.type", "nka": "$ngrm.nka",
      "_id": 0, "count": "$count", "pubs": "$pubs", }}]
  return pipeline


def get_top_topics_publications_pipeline(
  topn: Optional[int], author: Optional[str], cited: Optional[str],
  citing: Optional[str], probability:Optional[float]
):
  pipeline = [
    {'$match': {"topics": {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'cocit_authors': 0,
      "ngrams": 0}}, ]
  pipeline += filter_by_pubs_acc(author, cited, citing)

  pipeline += [{'$unwind': '$topics'},]

  if probability:
    pipeline += [
      {'$match': {"topics.probability": {'$gte': probability}}},]

  pipeline += [
    {'$group': {
      '_id': '$topics._id', 'count': {'$sum': 1},
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


def get_top_topics_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str], probability:Optional[float]
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'ngrams': 0}},
  ]
  pipeline += filter_by_pubs_acc(author, cited, citing)

  pipeline += [
    {'$unwind': '$topics'},
  ]
  if probability :
    pipeline += [
      {'$match': {'topics.probability': {'$gte': probability}}},]

  pipeline += [
    {'$group': {
      '_id': '$topics._id', 'count': {'$sum': 1},
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


def get_top_ngramms_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str], nka:Optional[int], ltype:Optional[LType]
):
  pipeline = [
    {'$match': {'ngrams._id': {'$exists': 1}}},
    {'$project': {'prefix': 0, 'suffix': 0, 'exact': 0, 'topics': 0}},
  ]
  pipeline += filter_by_pubs_acc(author, cited, citing)

  pipeline += [
    {'$unwind': '$ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]

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


def get_ngramm_filter(
  nka: Optional[int], ltype:Optional[LType], ngrm_field:Optional[str]=None
):
  ltype_str:str = ltype.value if ltype is not None else ''

  if not ngrm_field:
    return {
      '$match': {f: v for f, v in (('nka', nka), ('type', ltype_str)) if v}}

  return {
    '$match': {
      f'{ngrm_field}.{f}': v
      for f, v in (('nka', nka), ('type', ltype_str)) if v}}


def get_top_cocitauthors_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str]
):
  pipeline = [
    {'$match': {'frag_num': {'$gt': 0}, 'cocit_authors': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'bundles': 0, 'ngrams': 0, 'topics': 0}},]

  pipeline += filter_by_pubs_acc(author, cited, citing)

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


def get_top_cocitrefs_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str]
):
  pipeline = [
    {'$match': {'frag_num': {'$gt': 0}, 'bundles': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'ngrams': 0, 'topics': 0}},]

  pipeline += filter_by_pubs_acc(author, cited, citing)
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


def get_refauthors_part_pipeline(
  topn:int, author:Optional[str], cited:Optional[str], citing:Optional[str]
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'topics': 0,
      'ngrams': 0}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
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


def get_ref_auth4ngramm_tops_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str]
):
  pipeline = [
    {'$match': {'exact': {'$exists': True}}},
    {'$project': {'prefix': False, 'suffix': False, 'exact': False, }},]

  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
        'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
        'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$unwind': '$bundle.authors'},
    {'$group': {
        '_id': '$bundle.authors', 'pubs': {'$addToSet': '$pubid'}, 'conts': {
          '$addToSet': {
            'cid': '$_id', 'topics': '$topics',
            'ngrams': '$ngrams'}}}},
    {'$project': {
        '_id': False, 'aurhor': '$_id', 'cits': {'$size': '$conts'},
        'pubs': {'$size': '$pubs'}, 'pubs_ids': '$pubs', 'conts': '$conts',
        'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
        'year': '$bundle.year', 'authors': '$bundle.authors',
        'title': '$bundle.title', }},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_ref_bund4ngramm_tops_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str]
):
  pipeline = [
    {'$match': {'exact': {'$exists': True}}},
    {'$project': {'prefix': False, 'suffix': False, 'exact': False, }},]

  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$group': {
      '_id': '$bundles', 'cits': {'$sum': 1},
      'pubs': {'$addToSet': '$pubid'}, 'conts': {
        '$addToSet': {
          'cid': '$_id', 'topics': '$topics',
          'ngrams': '$ngrams'}}}},
    {'$lookup': {
      'from': 'bundles', 'localField': '_id', 'foreignField': '_id',
      'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$project': {
      '_id': False, 'bundle': '$_id', 'cits': True,
      'pubs': {'$size': '$pubs'}, 'pubs_ids': '$pubs', 'conts': True,
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title', }},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}}, ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_cocitauthors_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str]
):
  pipeline = [
    {'$match': {'frag_num': {'$gt': 0}, 'cocit_authors': {'$exists': True}}},
    {'$project': {'prefix': False, 'suffix': False, 'exact': False}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
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


def get_frags_cocitauthors_cocitauthors_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str]
):
  pipeline = [
    {'$match': {
      'cocit_authors': {'$exists': 1}, 'frag_num': {'$exists': 1},}},
    {'$project': {'pubid': 1, 'cocit_authors': 1, 'frag_num': 1}},]
  if filter_pipiline := filter_by_pubs_acc(author, cited, citing):
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
    {"$match": {"$expr": {"$ne": ["$cocit_authors", "$cont.cocit_authors"]}}},
    {"$group": {
      "_id": {
        "cocitauthor1": {
          "$cond": [
            {"$gte": ["$cocit_authors", "$cont.cocit_authors"]},
            "$cont.cocit_authors", "$cocit_authors"]},
        "cocitauthor2": {
          "$cond": [
            {"$gte": ["$cocit_authors", "$cont.cocit_authors"]},
            "$cocit_authors", "$cont.cocit_authors"]},
        "cont_id": "$_id"},
      "cont": {"$first": {"pubid": "$pubid", "frag_num": "$frag_num"}}}
    },
    {"$sort": {"_id": 1}},
    {"$group": {
      "_id": {
        "cocitauthor1": "$_id.cocitauthor1",
        "cocitauthor2": "$_id.cocitauthor2"},
      "count": {"$sum": 1},
      "conts": {
        "$push": {
          "cont_id": "$_id.cont_id", "pubid": "$cont.pubid",
          "frag_num": "$cont.frag_num"}}}},
    {"$sort": {"count": -1, "_id": 1}},
    {"$project": {
        "_id": 1,
        "cocitpair": {
          "author1": "$_id.cocitauthor1",
          "author2": "$_id.cocitauthor2"},
        "count": "$count", "conts": "$conts"}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_cocitauthors_ngramms_pipeline(
  topn: Optional[int], author: Optional[str], cited: Optional[str],
  citing: Optional[str], nka: Optional[int], ltype: Optional[LType]
):
  pipeline = [
    {"$match": {
      "cocit_authors": {"$exists": 1}, "frag_num": {"$exists": 1},
      "ngrams": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "cocit_authors": 1, "frag_num": 1,
      "ngrams": 1}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {"$unwind": "$cocit_authors"},
    {"$unwind": "$ngrams"},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},]
  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]
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


def get_frags_cocitauthors_topics_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str], probability:Optional[float]
):
  pipeline = [
    {"$match": {
      "cocit_authors": {"$exists": 1}, "frag_num": {"$exists": 1},
      "topics": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "cocit_authors": 1, "frag_num": 1,
      "topics": 1}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {"$unwind": "$cocit_authors"},
    {"$unwind": "$topics"},]
  if probability:
    pipeline += [
      {"$match": {"topics.probability": {"$gte": probability}}, }]
  pipeline += [
    {"$group": {
      "_id": {
        "cocit_authors": "$cocit_authors", "topic": "$topics._id",
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


def get_frags_ngramms_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str], nka:Optional[int], ltype:Optional[LType]
):
  pipeline = [
    {'$match': {
      'frag_num': {'$exists': 1}, 'ngrams': {'$exists': 1}}},
    {'$project': {
      'pubid': 1, 'frag_num': 1, 'linked_paper': '$ngrams'}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
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

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngramm')]

  pipeline += [
    {'$project': {
      'title': '$ngramm.title', 'type': '$ngramm.type', 'nka': '$ngramm.nka',
      'count': '$count', 'frags': '$frags'}}]

  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_ngramms_cocitauthors_pipeline(
  topn: Optional[int], author: Optional[str], cited: Optional[str],
  citing: Optional[str], nka: Optional[int], ltype: Optional[LType]
):
  pipeline = [
    {"$match": {
      "cocit_authors": {"$exists": 1}, "frag_num": {"$exists": 1},
      "ngrams": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "cocit_authors": 1, "frag_num": 1,
      "ngrams": 1}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {"$unwind": "$cocit_authors"},
    {"$unwind": "$ngrams"},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},]
  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]
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
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str], nka: Optional[int], ltype: Optional[LType]
):
  pipeline = [
    {'$match': {'ngrams': {'$exists': 1}, 'frag_num': {'$gt': 0}}},]

  pipeline += filter_by_pubs_acc(author, cited, citing)
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

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngramm')]

  pipeline += [
    {'$project': {
      'title': '$ngramm.title', 'type': '$ngramm.type', "nka": "$ngramm.nka",
      'count': 1, 'conts': '$cont_ids'}}
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_ngramms_ngramms_branch_pipeline(
  nka: Optional[int], ltype: Optional[LType]
):
  pipeline = [
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'topics': 0, 'bundles': 0}},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'cont'}},
    {'$unwind': '$cont'},
  ]

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'cont')]

  pipeline += [
    {'$unwind': '$ngrams'},
    {'$match': {'$expr': {'$eq': ['$ngrams._id', '$cont._id']}}},
  ]
  return pipeline


def get_frags_ngramms_topics_pipeline(
  topn: Optional[int], author: Optional[str], cited: Optional[str],
  citing: Optional[str], nka: Optional[int], ltype: Optional[LType],
  probability: Optional[float]
):
  pipeline = [
    {"$match": {
      "topics": {"$exists": 1}, "frag_num": {"$exists": 1},
      "ngrams": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "frag_num": 1, "topics": 1,
      "ngrams": 1}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {"$unwind": "$ngrams"},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]
  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]
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
      'ngrm': {'$first': "$ngrm"},}},
    {'$lookup': {
      'from': 'topics', 'localField': '_id.topic', 'foreignField': '_id',
      'as': 'topic'}},
    {"$unwind": '$topic'},
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
  topn: Optional[int], author: Optional[str],
  cited: Optional[str], citing: Optional[str]
):
  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': 1}, 'cocit_authors': {'$exists': 1}}},
    {'$project': {
      'pubid': 1, 'positive_negative': 1, 'cocit_authors': 1, 'frag_num': 1}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
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
    {'$match': {'$expr': {'$ne': ['$cocit_authors', '$cont.cocit_authors']}}},
    {'$group': {
      '_id': {
        'cocitauthor1': {'$cond': [
          {'$gte': ['$cocit_authors', '$cont.cocit_authors'],},
          '$cont.cocit_authors', '$cocit_authors']},
        'cocitauthor2': {'$cond': [
          {'$gte': ['$cocit_authors', '$cont.cocit_authors'],},
          '$cocit_authors', '$cont.cocit_authors']},
        'cont_id': '$_id'},
      'cont': {'$first': {
        'pubid': '$pubid', 'frag_num': '$frag_num',
        'positive_negative': '$positive_negative'}},}},
    {'$sort': {'_id': 1}},
    {'$group': {
      '_id': {
        'cocitauthor1': '$_id.cocitauthor1',
        'cocitauthor2': '$_id.cocitauthor2'},
      'count': {'$sum': 1},
      'conts': {'$push': {
        'cont_id': '$_id.cont_id', 'pubid': '$cont.pubid',
        'frag_num': '$cont.frag_num',
        'positive_negative': '$cont.positive_negative',},},}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$project': {
      '_id': False,
      'cocitpair': {
        'author1': '$_id.cocitauthor1', 'author2': '$_id.cocitauthor2'},
      'count': '$count', 'conts': '$conts', }},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frag_pos_neg_contexts(
  author:Optional[str], cited:Optional[str],
  citing:Optional[str]
):
  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': 1}, 'frag_num': {'$exists': 1}}},
    {'$project': {'pubid': 1, 'positive_negative': 1, 'frag_num': 1}},
  ]
  pipeline += filter_by_pubs_acc(author, cited, citing)
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


def get_frags_topics_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str], probability:Optional[float]
):
  pipeline = [
    {'$match': {
      'frag_num': {'$exists': 1}, 'topics': {'$exists': 1}}},
    {'$project': {
      'pubid': 1, 'frag_num': 1, 'linked_paper': '$topics'}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {'$unwind': '$linked_paper'},
  ]
  if probability :
    pipeline += [
      {'$match': {'linked_paper.probability': {'$gte': probability}}},]

  pipeline += [
    {'$group': {
      '_id': {'_id': '$linked_paper._id', 'frag_num': '$frag_num'},
      'count': {'$sum': 1,}}},
    {'$group': {
      '_id': '$_id._id', 'count': {'$sum': '$count'},
      'frags': {'$push': {'frag_num': '$_id.frag_num', 'count': '$count',}},}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_frags_topics_cocitauthors_pipeline(
  author:Optional[str], cited:Optional[str], citing:Optional[str],
  probability:Optional[float]
):
  pipeline = [
    {"$match": {
      "cocit_authors": {"$exists": 1}, "frag_num": {"$exists": 1},
      "topics": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "cocit_authors": 1, "frag_num": 1,
      "topics": 1}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {"$unwind": "$cocit_authors"},
    {"$unwind": "$topics"},]
  if probability:
    pipeline += [
      {"$match": {"topics.probability": {"$gte": probability}}, }]
  pipeline += [
    {"$group": {
      "_id": {
        "cocit_authors": "$cocit_authors", "topic": "$topics._id",
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


def get_frags_topics_ngramms_pipeline(
  author: Optional[str], cited: Optional[str], citing: Optional[str],
  nka: Optional[int], ltype: Optional[LType], probability: Optional[float],
  topn_crpssgramm:Optional[int]
):
  pipeline = [
    {"$match": {
      "topics": {"$exists": 1}, "frag_num": {"$exists": 1},
      "ngrams": {'$exists': 1}}},
    {"$project": {
      "pubid": 1, "topics": 1, "frag_num": 1,
      "ngrams": 1}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {"$unwind": "$ngrams"},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]
  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]
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
        "topic": "$topics.title",
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


def get_frags_topics_topics_pipeline(
  author: Optional[str], cited: Optional[str], citing: Optional[str],
  probability: Optional[float]
):
  pipeline = [
    {"$match": {
      "frag_num": {"$exists": 1}, "topics": {'$exists': 1}}},
    {"$project": {"pubid": 1, "frag_num": 1, "topics": 1}},
  ]
  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {"$project": {"pub": 0}},
    {"$unwind": "$topics"},]
  if probability:
    pipeline += [
      {"$match": {
        "topics.probability": {"$gte": probability}}, }]
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
  pipeline += [
    {'$project': {
      "frag_num": 1, "topic1": "$topics._id",
      "topic2": "$cont.topics._id"}},
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


def get_pos_neg_cocitauthors_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str]
):
  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': True},
      'cocit_authors': {'$exists': True}}},
    {'$project': {
      'pubid': True, 'positive_negative': True, 'cocit_authors': True}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
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


def get_pos_neg_contexts_pipeline(
  author:Optional[str], cited:Optional[str], citing:Optional[str]
):
  pipeline = [
    {'$match': {'positive_negative': {'$exists': True}}},
    {'$project': {'pubid': True, 'positive_negative': True}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
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


def get_pos_neg_ngramms_pipeline(
  topn: Optional[int], author: Optional[str], cited: Optional[str],
  citing: Optional[str], nka:Optional[int], ltype:Optional[LType]
):
  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': True},
      'ngrams': {'$exists': True},}},
    {'$project': {
      'pubid': True, 'positive_negative': True, 'ngrams': True}},]
  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [
    {'$unwind': '$ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},]
  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]
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


def get_pos_neg_pubs_pipeline(
  author: Optional[str], cited: Optional[str], citing: Optional[str]
):
  pipeline = [
    {'$match': {'positive_negative': {'$exists': 1}, }},]
  if filter_pipeline := filter_by_pubs_acc(author, cited, citing):
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


def get_pos_neg_topics_pipeline(
  author: Optional[str], cited: Optional[str], citing: Optional[str],
  probability:Optional[float]
):
  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': 1},
      'topics': {'$exists': 1}}},
    {'$project': {
      'pubid': 1, 'positive_negative': 1, 'topics': 1}},]
  if filter_pipeline := filter_by_pubs_acc(author, cited, citing):
    pipeline += filter_pipeline

  pipeline += [
    {'$unwind': '$topics'},
    {'$match': {'topics.probability': {'$gte': probability}}},
    {'$group': {
      '_id': {
        'pos_neg': {
          '$arrayElemAt': [
            ['neutral', 'positive', 'positive', 'negative', 'negative'],
            '$positive_negative.val']},
        'topic': '$topics._id'},
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


def get_publications_cocitauthors_pipeline(
  author: Optional[str], cited: Optional[str], citing: Optional[str],
  topn_auth:Optional[int]
):
  pipeline = [
    {"$match": {"cocit_authors": {"$exists": 1}}}]
  if filter_pipeline := filter_by_pubs_acc(author, cited, citing):
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


def get_publications_ngramms_pipeline(
  topn:Optional[int], author: Optional[str], cited: Optional[str],
  citing: Optional[str], nka:Optional[int], ltype:Optional[LType],
  topn_gramm:Optional[int]
):
  pipeline = [
    {"$match": {"ngrams": {"$exists": 1}}}]
  if filter_pipeline := filter_by_pubs_acc(author, cited, citing):
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
  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]

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


def get_publications_topics_topics_pipeline(
  author: Optional[str], cited: Optional[str], citing: Optional[str],
  probability:Optional[float]
):
  pipeline = [
    {"$match": {
      "pubid": {"$exists": 1}, "topics": {'$exists': 1}}},
    {"$project": {"pubid": 1, "topics": 1}}, ]
  pipeline += filter_by_pubs_acc(author, cited, citing)
  pipeline += [{"$project": {"pub": 0}}, {"$unwind": "$topics"}, ]
  if probability:
    pipeline += [{
      "$match": {
        "topics.probability": {"$gte": probability}}, }]
  pipeline += [{
    '$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}}, {'$unwind': "$cont"},
    {'$unwind': "$cont.topics"}, ]
  if probability:
    pipeline += [{
      "$match": {
        "cont.topics.probability": {"$gte": probability}}, }]
  pipeline += [{
    '$project': {
      "pubid": 1, "topic1": "$topics._id",
      "topic2": "$cont.topics._id"}},
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


def get_top_detail_bund_refauthors(
  topn: Optional[int], author: Optional[str], cited: Optional[str],
  citing: Optional[str]
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}, 'bundles': {'$exists': 1}}},]
  if filter_pipeline := filter_by_pubs_acc(author, cited, citing):
    pipeline += filter_pipeline

  pipeline += [
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'topics': 0,
      'ngrams': 0}},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
      'as': 'bun'}},
    {'$unwind': '$bun'},
    {'$unwind': '$bun.authors'},
    {'$group': {
      '_id': {'author': '$bun.authors', 'bund': '$bundles'},
      'cits': {'$addToSet': '$_id'}, 'cits_all': {'$sum': 1},
      'pubs': {'$addToSet': '$pubid'}, 'bunds': {
        '$addToSet': {
          '_id': '$bun._id', 'total_cits': '$bun.total_cits',
          'total_pubs': '$bun.total_pubs'}}, }},
    {'$unwind': '$bunds'},
    {'$group': {
      '_id': '$_id.author', 'cits_all': {'$sum': '$cits_all'},
      'cits': {'$push': '$cits'}, 'pubs': {'$push': '$pubs'},
      'bunds': {
        '$push': {
          '_id': '$bunds._id', 'cnt': '$cits_all',
          'total_cits': '$bunds.total_cits',
          'total_pubs': '$bunds.total_pubs'}}, }},
    {'$project': {
      '_id': 0, 'author': '$_id', 'cits_all': '$cits_all',
      'cits': {
        '$size': {
          '$reduce': {
            'input': '$cits', 'initialValue': [],
            'in': {'$setUnion': ['$$value', '$$this']}}}, }, 'pubs': {
        '$size': {
          '$reduce': {
            'input': '$pubs', 'initialValue': [],
            'in': {'$setUnion': ['$$value', '$$this']}}}},
      'bunds_cnt': {'$size': '$bunds'},
      'total_cits': {'$sum': '$bunds.total_cits'},
      'total_pubs': {'$sum': '$bunds.total_pubs'},
      'bundles': {
        '$map': {
          'input': '$bunds', 'as': 'b',
          'in': {'bundle': '$$b._id', 'cnt': '$$b.cnt'}, }}, }},
    {'$sort': {
      'cits_all': -1, 'cits': -1, 'bunds_cnt': -1, 'pubs': -1, 'author': 1}}, ]

  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline
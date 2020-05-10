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
      'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_topics': 0,
      'linked_papers_ngrams': 0}},
  ]

  pipeline += _filter_by_pubs_acc(author, cited, citing)

  pipeline += [
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},  ##
    {'$group': {
      '_id': '$bundles', 'cits': {'$sum': 1}, 'pubs': {'$addToSet': '$pub_id'},
      'pos_neg': {'$push': '$positive_negative'},
      'frags': {'$push': '$frag_num'}, }},
    {'$project': {
        'cits': 1, 'pubs': {'$size': '$pubs'}, 'pos_neg': 1, 'frags': 1}},
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


def _filter_by_pubs_acc(
  author:Optional[str], cited:Optional[str], citing:Optional[str],
):
  if not any([author, cited, citing]):
    return []
  match = _filter_acc_dict(author, cited, citing)
  pipeline = [
    {'$lookup': {
      'from': 'publications', 'localField': 'pubid', 'foreignField': '_id',
      'as': 'pub'}},
    {'$unwind': '$pub'},
    # {'$match': {'pub.uni_authors': {'$exists': 1}}},
    {'$match': {f'pub.{key}': val for key, val in match.items()}},
  ]
  return pipeline


def _filter_acc_dict(
  author:Optional[str], cited:Optional[str], citing:Optional[str],
):
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
      'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_topics': 0,
      'linked_papers_ngrams': 0}},
    {'$unwind': '$bundles'},
  ]

  pipeline += _filter_by_pubs_acc(author, cited, citing)

  pipeline += [
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
      'as': 'bun'}},
    {'$unwind': '$bun'},
    {'$unwind': '$bun.authors'},
    {'$group': {
      '_id': '$bun.authors', 'cits': {'$addToSet': '$_id'},
      'cits_all': {'$sum': 1}, 'pubs': {'$addToSet': '$pub_id'},
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

  if filter := _filter_acc_dict(author, cited, citing):
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


def get_top_topics_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str], probability:Optional[float]
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_ngrams': False}},
  ]
  pipeline += _filter_by_pubs_acc(author, cited, citing)

  pipeline += [
    {'$unwind': '$linked_papers_topics'},
  ]
  if probability :
    pipeline += [
      {'$match': {'linked_papers_topics.probability': {'$gte': probability}}},]

  pipeline += [
    {'$group': {
      '_id': '$linked_papers_topics._id', 'count': {'$sum': 1},
      'probability_avg': {'$avg': '$linked_papers_topics.probability'},
      'probability_stddev': {'$stdDevPop': '$linked_papers_topics.probability'},
      'conts': {'$addToSet': '$_id'}, }},
    {'$sort': {'count': -1, '_id': 1}}
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  return pipeline


def get_top_ngramms_pipeline(
  topn:Optional[int], author:Optional[str], cited:Optional[str],
  citing:Optional[str], nka:Optional[int], ltype:Optional[LType]
):
  pipeline = [
    {'$match': {'linked_papers_ngrams._id': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False}},
  ]
  pipeline += _filter_by_pubs_acc(author, cited, citing)

  pipeline += [
    {'$unwind': '$linked_papers_ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]

  gident = {'title': '$ngrm.title', 'type': '$ngrm.type'}

  pipeline += [
  {'$group': {
      '_id': {'title': '$ngrm.title', 'type': '$ngrm.type'},
      'count': {'$sum': '$linked_papers_ngrams.cnt'},
      'count_cont': {'$sum': 1},
      'conts': {
        '$push': {'cont_id': '$_id', 'cnt': '$linked_papers_ngrams.cnt'}}}},
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

# -*- codong: utf-8 -*-
import logging
from operator import itemgetter
from typing import Optional, Tuple

from pymongo.collection import Collection


_logger = logging.getLogger('cirtec')


async def _get_topn_cocit_authors(
  contexts, topn:Optional[int], *, include_conts:bool=False
):
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

  pipeline = [
    {'$match': {'frag_num': {'$gt': 0}, 'cocit_authors': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'bundles': 0, 'linked_papers_ngrams': 0, 'linked_papers_topics': 0}},
    {'$unwind': '$cocit_authors'},
    # {'$group': {'_id': '$cocit_authors', 'count': {'$sum': 1}}},
    group,
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  # _logger.debug('pipeline: %s', pipeline)
  top50 = [get_as_tuple(doc) async for doc in contexts.aggregate(pipeline)]
  return tuple(top50)


async def _get_topn_cocit_refs(
  contexts, topn:Optional[int], *, include_conts:bool=False,
  include_descr:bool=False
):
  """Получить самых со-цитируемых референсов"""
  # get_as_tuple = itemgetter('_id', 'count', 'conts', 'bundles')
  if include_conts:
    group = {
      '$group': {
        '_id': '$cocit_refs', 'count': {'$sum': 1},
        'conts': {'$addToSet': '$_id'}}}
  else:
    group = {'$group': {'_id': '$cocit_refs', 'count': {'$sum': 1}}}

  pipeline = [
    {'$match': {'frag_num': {'$gt': 0}, 'cocit_refs': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'bundles': 0, 'linked_papers_ngrams': 0, 'linked_papers_topics': 0}},
    {'$unwind': '$cocit_refs'},
    group,
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  if include_descr:
    pipeline += [
      {'$lookup': {
        'from': 'bundles', 'localField': '_id', 'foreignField': '_id',
        'as': 'bundles'}},
      {'$unwind': '$bundles'},
    ]

  # _logger.debug('pipeline: %s', pipeline)
  # top50 = [doc async for doc in contexts.aggregate(pipeline)]
  # return tuple(top50)
  async for doc in contexts.aggregate(pipeline):
    yield doc


async def _get_topn_ngramm(
  colls:Collection, nka:Optional[int], ltype:Optional[str], topn:Optional[int],
  *, pub_id:Optional[str]=None, title_always_id:bool=False, show_type:bool=False
) -> Tuple:

  if pub_id:
    pipeline = [{'$match': {'pub_id': pub_id}}]
  else:
    pipeline = []

  pipeline += [
    {'$match': {'linked_papers_ngrams': {'$exists': True}}},
    {'$project': {
      '_id': 1, 'pub_id': 1, 'linked_paper': '$linked_papers_ngrams'}},
    {'$unwind': '$linked_paper'},
    {'$group': {
      '_id': '$linked_paper._id', 'count': {'$sum': '$linked_paper.cnt'},
      'pub_ids': {'$addToSet': '$pub_id'}, 'cont_ids': {'$addToSet': '$_id'},}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$lookup': {
      'from': 'n_gramms', 'localField': '_id', 'foreignField': '_id',
      'as': 'ngramm'}},
    {'$unwind': '$ngramm'},
  ]

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngramm')]

  if ltype and not title_always_id:
    title = '$ngramm.title'
  else:
    title = '$_id'

  projects = {
    '_id': 0, 'type': '$ngramm.type', 'title': title, 'count': 1,
    'conts': '$cont_ids'}

  pipeline += [{'$project': projects}]

  if topn:
    pipeline += [{'$limit': topn}]

  # _logger.debug('pipeline: %s', pipeline)
  get_as_tuple = (
    itemgetter('title', 'type', 'count', 'conts') if show_type
    else itemgetter('title', 'count', 'conts'))
  contexts:Collection = colls.database.contexts
  topN = tuple([get_as_tuple(doc) async for doc in contexts.aggregate(pipeline)])
  return topN


async def _get_topn_topics(
  colls:Collection, topn:Optional[int], *, pub_id:Optional[str]=None
) -> Tuple:
  """Получение наиболее часто встречающихся топиков"""
  pipeline = [
    {'$lookup': {
      'from': 'contexts', 'localField': '_id',
      'foreignField': 'linked_papers_topics._id', 'as': 'cont'}},
    {'$unwind': '$cont'},
    {'$project': {
      'title': 1, 'linked_paper': '$cont.linked_papers_topics',
      'cont_id': '$cont._id', 'pub_id': '$cont.pub_id'}},
    {'$unwind': '$linked_paper'},
    {'$match': {'$expr': {'$eq': ['$_id', '$linked_paper._id']}}}
  ]

  if pub_id:
    pipeline += [{'$match': {'pub_id': pub_id}}]

  pipeline += [
    {'$group': {'_id': '$title', 'count': {'$sum': 1},
      'conts': {'$addToSet': '$cont_id'}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  # _logger.debug('pipeline: %s', pipeline)
  get_as_tuple = itemgetter('_id', 'count', 'conts')
  topN = tuple([get_as_tuple(doc) async for doc in colls.aggregate(pipeline)])

  return topN


def get_ngramm_filter(
  nka: Optional[int], ltype: Optional[str],
  ngrm_field: Optional[str]=None
):
  if not ngrm_field:
    return {'$match': {f: v for f, v in (('nka', nka), ('type', ltype)) if v}}

  return {
    '$match': {
      f'{ngrm_field}.{f}': v for f, v in (('nka', nka), ('type', ltype)) if v}}


def _get_refauthors_pipeline(topn:int=None):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_topics': 0,
      'linked_papers_ngrams': 0}},
    {'$unwind': '$bundles'},
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

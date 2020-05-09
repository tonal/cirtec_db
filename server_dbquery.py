# -*- codong: utf-8 -*-
import logging
from typing import Optional


_logger = logging.getLogger('cirtec')


def get_refbindles_pipeline(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_topics': 0,
      'linked_papers_ngrams': 0}},
  ]

  pipeline = _filter_by_pubs_acc(author, cited, citing)

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


def _filter_by_pubs_acc(author, cited, citing):
  if not any([author, cited, citing]):
    return []
  match = {key: val for key, val in
    (('authors', author), ('cited', cited), ('citing', citing)) if val}
  pipeline = [
    {'$lookup': {
      'from': 'publications', 'localField': 'pubid', 'foreignField': '_id',
      'as': 'pub'}},
    # {'$match': {'pub.uni_authors': {'$exists': 1}}},
    {'$match': {f'pub.uni_{key}': val for key, val in match.items()}}
  ]
  return pipeline


def get_refauthors_pipeline(topn: Optional[int] = None,
  author: Optional[str] = None, cited: Optional[str] = None,
  citing: Optional[str] = None
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_topics': 0,
      'linked_papers_ngrams': 0}},
    {'$unwind': '$bundles'},
  ]

  pipeline = _filter_by_pubs_acc(author, cited, citing)

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

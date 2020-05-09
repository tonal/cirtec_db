# -*- codong: utf-8 -*-
import logging
from typing import Optional


_logger = logging.getLogger('cirtec')


def _get_refbindles_pipeline(
  topn:int=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_topics': 0,
      'linked_papers_ngrams': 0}},
  ]
  if any([author, cited, citing]):
    match = {
      key: val for key, val in (
        ('authors', author), ('cited', cited), ('citing', citing)
      ) if val}
    pipeline += [
      {'$lookup': {
          'from': 'publications', 'localField': 'pubid', 'foreignField': '_id',
          'as': 'pub'}},
      # {'$match': {'pub.uni_authors': {'$exists': 1}}},
      {'$match': {f'pub.uni_{key}': val for key, val in match.items()}}
    ]

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

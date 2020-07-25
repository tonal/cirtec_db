#! /usr/bin/env python3
# -*- codong: utf-8 -*-
from typing import Optional

from models_dev.common import filter_by_pubs_acc
from models_dev.models import AuthorParam


def get_ref_auth4ngramm_tops(
  topn:Optional[int], authorParams: AuthorParam
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {'prefix': 0, 'suffix': 0, 'exact': 0, }},]

  pipeline += filter_by_pubs_acc(authorParams)
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
            'cid': '$_id', 'topics': '$topics', 'ngrams': '$ngrams'}}}},
    {'$project': {
        '_id': 0, 'aurhor': '$_id', 'cits': {'$size': '$conts'},
        'pubs': {'$size': '$pubs'}, 'pubs_ids': '$pubs', 'conts': '$conts',
        'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
        'year': '$bundle.year', 'authors': '$bundle.authors',
        'title': '$bundle.title', }},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_ref_bund4ngramm_tops(
  topn:Optional[int], authorParams: AuthorParam
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {'prefix': 0, 'suffix': 0, 'exact': 0, }},]

  pipeline += filter_by_pubs_acc(authorParams)
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
      '_id': 0, 'bundle': '$_id', 'cits': 1,
      'pubs': {'$size': '$pubs'}, 'pubs_ids': '$pubs', 'conts': 1,
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title', }},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}}, ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


def get_top_detail_bund_refauthors(
  topn: Optional[int], authorParams: AuthorParam
):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}, 'bundles': {'$exists': 1}}},]
  if filter_pipeline := filter_by_pubs_acc(authorParams):
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

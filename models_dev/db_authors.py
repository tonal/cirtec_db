#! /usr/bin/env python3
from typing import Optional

from models_dev.common import get_ngramm_filter
from models_dev.models import AType, NgrammParam


def get_publics(
  atype:AType, ngpr:NgrammParam, probability:float=.5
) -> list:
  assert not ngpr.is_empty()
  assert probability > 0
  aval = 'authors' if atype == AType.author else atype.value
  field = f'$uni_{aval}' # '$uni_authors'
  pipeline = [
    # {'$group': {'_id': field, 'cnt': {'$sum': 1}}},
    {'$unwind': field}, # '$uni_authors'},
    {'$facet': {
      'publications': [
        {'$group': {'_id': field, 'cnt': {'$sum': 1}}}, ],
      'references': [
        {'$unwind': '$refs'},
        {'$match': {'refs.bundle': {'$exists': 1}}},
        {'$group': {'_id': field, 'cnt': {'$sum': 1}}}, ],
      'ref_authors': [
        {'$unwind': '$refs'},
        {'$match': {'refs.bundle': {'$exists': 1}}},
        {'$unwind': '$refs.authors'},
        {'$group': {
          '_id': {'pub_author': field, 'ref_author': '$refs.authors'}}},
        {'$group': {'_id': '$_id.pub_author', 'cnt': {'$sum': 1}}}, ],
      'conts': [
        {'$lookup': {
          'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
          'as': 'cont'}},
        {'$unwind': '$cont'},
        {'$group': {
          '_id': {'pub_author': '$uni_authors', 'cont': '$cont._id'}}},
        {'$group': {'_id': '$_id.pub_author', 'cnt': {'$sum': 1}}}, ],
      'ngramms': [
        {'$lookup': {
          'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
          'as': 'cont'}},
        {'$unwind': '$cont'},
        {'$unwind': '$cont.ngrams'},
        {'$lookup': {
          'from': 'n_gramms', 'localField': 'cont.ngrams._id',
          'foreignField': '_id', 'as': 'ngrm'}},
        {'$unwind': '$ngrm'},
        # {'$match': {'ngrm.nka': 2, 'ngrm.type': 'lemmas'}},
        get_ngramm_filter(ngpr, 'ngrm'),
        {'$group': {'_id': {'pub_author': field, 'ngrm': '$ngrm._id'}}},
        {'$group': {'_id': '$_id.pub_author', 'cnt': {'$sum': 1}}}, ],
      # 'topics': [
      #   {'$lookup': {
      #     'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
      #     'as': 'cont'}},
      #   {'$unwind': '$cont'},
      #   {'$unwind': '$cont.topics'},
      #   {'$match': {'cont.topics.probability': {'$gte': probability}}},
      #   {'$group': {
      #     '_id': {'pub_author': field, 'topic': '$cont.topics._id'}}},
      #   {'$group': {'_id': '$_id.pub_author', 'cnt': {'$sum': 1}}}, ],
    }},
    {'$unwind': '$publications'},
    {'$unwind': '$references'},
    {'$match': {'$expr': {'$eq': ['$publications._id', '$references._id']}}},
    {'$unwind': '$ref_authors'},
    {'$match': {'$expr': {'$eq': ['$publications._id', '$ref_authors._id']}}},
    {'$unwind': '$conts'},
    {'$match': {'$expr': {'$eq': ['$publications._id', '$conts._id']}}},
    {'$unwind': '$ngramms'},
    {'$match': {'$expr': {'$eq': ['$publications._id', '$ngramms._id']}}},
    # {'$unwind': '$topics'},
    # {'$match': {'$expr': {'$eq': ['$publications._id', '$topics._id']}}},
    {'$project': {
      'author': '$publications._id', 'cnt_pubs': '$publications.cnt',
      'cnt_refs': '$references.cnt', 'cnt_ref_auths': '$ref_authors.cnt',
      'cnt_conts': '$conts.cnt', 'cnt_ngramms': '$ngramms.cnt',}},
      # 'cnt_topics': '$topics.cnt'}},
    {'$sort': {'author': 1}},
  ]
  return pipeline

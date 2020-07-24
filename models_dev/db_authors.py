#! /usr/bin/env python3
from typing import Optional

from pydantic import validate_arguments

from models_dev.common import get_ngramm_filter
from models_dev.models import AType, NgrammParam, AuthorParam


def get_publics(
  atype:AType, ngpr:NgrammParam, probability:float=.5
) -> list:
  assert not ngpr.is_empty()
  assert probability > 0
  field = '$' + _atype2field(atype)
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
        get_ngramm_filter(ngpr, 'cont.ngrams'),
        {'$group': {'_id': {'pub_author': field, 'ngrm': '$cont.ngrams._id'}}},
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
      'cnt_contexts': '$conts.cnt', 'cnt_ngramms': '$ngramms.cnt',}},
      # 'cnt_topics': '$topics.cnt'}},
    {'$sort': {'author': 1}},
  ]
  return pipeline


@validate_arguments
def _atype2field(atype:AType) -> str:
  key = 'authors' if atype == AType.author else atype.value
  field = f'uni_{key}'  # '$uni_authors'
  return field


def get_cmp_authors(ap1:AuthorParam, ap2:AuthorParam) -> list:
  assert ap1.only_one() and ap2.only_one()
  atype1, name1 = ap1.get_qual_auth()
  atype2, name2 = ap2.get_qual_auth()

  pipiline = []

  if atype1 == atype2:
    field = _atype2field(atype1)
    pipiline += [
      {'$unwind': '$' + field},
      {'$match': {field: {'$in': [name1, name2]}}},
      {'$addFields': {'author': '$' + field, 'atype': atype1.value}},
      # {'$unwind': '$refs'},
      # {'$match': {'refs.bundle': {'$exists': 1}}},
      # {'$group': {
      #   '_id': {'author': '$author', 'atype': '$atype', 'bundle': '$refs.bundle'},
      #   'cnt': {'$sum': 1}}},
    ]
  else:
    field1 = _atype2field(atype1)
    field2 = _atype2field(atype2)
    pipiline += [
      {'$match': {'$or': [{field1: name1}, {field2: name2}]}},
      {'$facet': {
        'atype1': [
          {'$unwind': '$' + field1},
          {'$match': {field1: name1}},
          {'$addFields': {'author': '$' + field1, 'atype': atype1.value}}, ],
        'atype2': [
          {'$unwind': '$' + field2},
          {'$match': {field2: name2}},
          {'$addFields': {'author': '$' + field2, 'atype': atype2.value}}, ]
        }},
      {'$project': {'data': {'$setUnion': ['$atype1', '$atype2']}}},
      {'$unwind': '$data'},
      {'$project': {
        'refs': '$data.refs', 'author': '$data.author', 'atype': '$data.atype'}},
      # {'$unwind': '$data.refs'},
      # {'$match': {'data.refs.bundle': {'$exists': 1}}},
      # {'$group': {
      #   '_id': {
      #     'author': '$data.author', 'atype': '$data.atype',
      #     'bundle': '$data.refs.bundle'},
      #   'cnt': {'$sum': 1}}},
    ]
  pipiline += [
    {'$unwind': '$refs'},
    {'$match': {'refs.bundle': {'$exists': 1}}},
    {'$group': {
      '_id': {'author': '$author', 'atype': '$atype', 'bundle': '$refs.bundle'},
      'cnt': {'$sum': 1}}},
    {'$project': {
      '_id': 0, 'atype': '$_id.atype', 'name': '$_id.author',
      'bundle': '$_id.bundle', 'cnt': 1}},
    # {'$sort': {'cnt': -1, '_id': 1,}}
  ]

  return pipiline

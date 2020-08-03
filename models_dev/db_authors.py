#! /usr/bin/env python3
from enum import auto
from typing import Dict

from pydantic import validate_arguments

from models_dev.common import get_ngramm_filter
from models_dev.models import AType, AutoName, NgrammParam, AuthorParam


class FieldsSet(str, AutoName):
  bundle = auto()
  ref_author = auto()
  ngram = auto()
  topic = auto()
  topic_strong = auto()


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


def get_cmp_authors(
  ap1:AuthorParam, ap2:AuthorParam, ngrmpr:NgrammParam, probability:float
) -> Dict[str, list]:
  assert ap1.only_one() and ap2.only_one()
  atype1, name1 = ap1.get_qual_auth()
  atype2, name2 = ap2.get_qual_auth()
  pipelines = {}
  for fld_set in FieldsSet:
    # type: fld_set: FieldsSet
    pipeline = get_cmp_authors_ref(
      atype1, name1, atype2, name2, fld_set, ngrmpr, probability)
    pipelines[fld_set] = pipeline

  return pipelines


def get_cmp_authors_ref(
  atype1:AType, name1:str, atype2:AType, name2:str, field_col:FieldsSet,
  ngrmpr:NgrammParam, probability:float
) -> list:

  pipiline = []

  if atype1 == atype2:
    field = _atype2field(atype1)
    pipiline += [
      {'$unwind': '$' + field},
      {'$match': {field: {'$in': [name1, name2]}}},
      {'$addFields': {'author': '$' + field, 'atype': atype1.value}},
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
        '_id': '$data._id', 'refs': '$data.refs', 'author': '$data.author',
        'atype': '$data.atype'}},
    ]

  if field_col in {FieldsSet.bundle, FieldsSet.ref_author}:
    pipiline += [
      {'$unwind': '$refs'}, ]

    if field_col == FieldsSet.bundle:
      pipiline += [
        {'$match': {'refs.bundle': {'$exists': 1}}},
        {'$addFields': {'label': '$refs.bundle'}}, ]
    elif field_col == FieldsSet.ref_author:
      pipiline += [
        {'$unwind': '$refs.authors'},
        # {'$match': {'refs.bundle': {'$exists': 1}}},
        {'$addFields': {'label': '$refs.authors'}}, ]
    else:
      assert False
  elif field_col == FieldsSet.ngram:
    pipiline += [
      {'$lookup': {
        'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
        'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.ngrams'},
      {'$match': {
        'cont.ngrams.nka': ngrmpr.nka, 'cont.ngrams.type': ngrmpr.ltype.value}},
      {'$addFields': {'label': '$cont.ngrams._id'}},
    ]
  elif field_col == FieldsSet.topic:
    pipiline += [
      {'$lookup': {
        'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
        'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.topics'},
      {'$match': {'cont.topics.probability': {'$gte': 0.5}, }},
      {'$addFields': {'label': {'$split': ['$cont.topics.title', ', ']}}},
      {'$unwind': '$label'},
    ]
  elif field_col == FieldsSet.topic_strong:
    pipiline += [
      {'$lookup': {
        'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
        'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.topics'},
      {'$match': {'cont.topics.probability': {'$gte': 0.5}, }},
      {'$lookup': {
        'from': 'topics', 'localField': 'cont.topics._id', 'foreignField': '_id',
        'as': 'topic'}},
      {'$unwind': '$topic'},
      {'$match': {
        '$or': [
          {f'topic.uni_{atype1}': name1}, {f'topic.uni_{atype2}': name2}]}},
      {'$addFields': {'label': {'$split': ['$cont.topics.title', ', ']}}},
      {'$unwind': '$label'},
    ]

  pipiline += [
    {'$group': {
      '_id': {'author': '$author', 'atype': '$atype', 'label': '$label'},
      'cnt': {'$sum': 1}}},
    {'$project': {
      '_id': 0, 'atype': '$_id.atype', 'name': '$_id.author',
      'label': '$_id.label', 'cnt': 1}},
    # {'$sort': {'cnt': -1, '_id': 1,}}
  ]

  return pipiline

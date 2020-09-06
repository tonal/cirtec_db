#! /usr/bin/env python3
from collections import Counter, defaultdict
from enum import auto
from itertools import combinations
from operator import itemgetter
from typing import Dict, List, Optional

import numpy as np
from pydantic import validate_arguments
from scipy.spatial.distance import jensenshannon

from models_dev.common import get_ngramm_filter
from models_dev.models import AType, AutoName, NgrammParam, AuthorParam
from utils import get_logger_dev as get_logger


_logger = get_logger()


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
          '_id': {'pub_author': field, 'cont': '$cont._id'}}},
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
    {'$unwind': {'path': '$references', 'preserveNullAndEmptyArrays': True}},
    {'$match': {'$expr': {'$eq': ['$publications._id', '$references._id']}}},
    {'$unwind': {'path': '$ref_authors', 'preserveNullAndEmptyArrays': True}},
    {'$match': {'$expr': {'$eq': ['$publications._id', '$ref_authors._id']}}},
    {'$unwind': {'path': '$conts', 'preserveNullAndEmptyArrays': True}},
    {'$match': {'$expr': {'$eq': ['$publications._id', '$conts._id']}}},
    {'$unwind': {'path': '$ngramms', 'preserveNullAndEmptyArrays': True}},
    {'$match': {'$expr': {'$eq': ['$publications._id', '$ngramms._id']}}},
    # {'$unwind': {'path': '$topics', 'preserveNullAndEmptyArrays': True}},
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
  elif field_col in {FieldsSet.topic, FieldsSet.topic_strong}:
    pipiline += [
      {'$lookup': {
        'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
        'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.topics'},
      {'$match': {'cont.topics.probability': {'$gte': probability}, }},
    ]
    if field_col == FieldsSet.topic_strong:
      pipiline += [
        {'$lookup': {
          'from': 'topics', 'localField': 'cont.topics._id',
          'foreignField': '_id', 'as': 'topic'}},
        {'$unwind': '$topic'},
        {'$match': {
            '$or': [{f'topic.uni_{atype1}': name1},
              {f'topic.uni_{atype2}': name2}]}},
      ]

    pipiline += [
      {'$addFields': {'label': {'$split': ['$cont.topics.title', ', ']}}},
      {'$unwind': '$label'},
      {'$group': {
        '_id': {
          'author': "$author", 'atype': "$atype", 'label': "$label",
          'cont': "$cont._id"}, }},
      {'$project': {
        '_id': 0, 'author': "$_id.author", 'atype': "$_id.atype",
        'label': "$_id.label", 'cont': '$_id.cont'}},
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


def get_cmp_authors_cont(
  ap1:AuthorParam, ap2:AuthorParam, word:str, field_col:FieldsSet,
  ngrmpr:Optional[NgrammParam], probability:Optional[float]
) -> list:
  assert ap1.only_one() and ap2.only_one()
  atype1, name1 = ap1.get_qual_auth()
  atype2, name2 = ap2.get_qual_auth()
  return get_cmp_authors_ref_cont(
    atype1, name1, atype2, name2, word, field_col, ngrmpr, probability)


def get_cmp_authors_ref_cont(
  atype1:AType, name1:str, atype2:AType, name2:str, word:str,
  field_col:FieldsSet, ngrmpr:Optional[NgrammParam], probability:Optional[float]
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
        {'$addFields': {'label': '$refs.authors'}}, ]
    else:
      assert False

    if word:
      pipiline += [
        {'$match': {'label': word}}, ]

    pipiline += [
      {'$lookup': {
        'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
        'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.refs'},
      {'$match': {'$expr': {'$eq': ['$refs.num', '$cont.refs.num']}}},
    ]

  elif field_col == FieldsSet.ngram:
    ltype = ngrmpr.ltype.value
    pipiline += [
      {'$lookup': {
        'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
        'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.ngrams'},
      {'$match': {
        'cont.ngrams.nka': ngrmpr.nka, 'cont.ngrams.type': ltype}},
      {'$addFields': {'label': '$cont.ngrams._id'}},
    ]
    if word:
      pipiline += [
        {'$match': {'label': f'{ltype}_{word}'}}, ]

  elif field_col == FieldsSet.topic:
    pipiline += [
      {'$lookup': {
        'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
        'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.topics'},
      {'$match': {'cont.topics.probability': {'$gte': probability}, }},
      {'$addFields': {'label': {'$split': ['$cont.topics.title', ', ']}}},
      {'$unwind': '$label'},
    ]
    if word:
      pipiline += [
        {'$match': {'label': word}}, ]

  elif field_col == FieldsSet.topic_strong:
    pipiline += [
      {'$lookup': {
        'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
        'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.topics'},
      {'$match': {'cont.topics.probability': {'$gte': probability}, }},
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
    if word:
      pipiline += [
        {'$match': {'label': word}}, ]

  pipiline += [
    {'$group': {
      '_id': {'author': '$author', 'atype': '$atype', 'label': '$label'},
      'conts': {'$addToSet': '$cont._id'}}},
    {'$project': {
      '_id': 0, 'atype': '$_id.atype', 'name': '$_id.author',
      'label': '$_id.label', 'conts': 1}},
    # {'$sort': {'cnt': -1, '_id': 1,}}
  ]

  return pipiline


async def calc_cmp_vals(
  atype1: AType, name1: str, atype2: AType, name2: str, curs, data_key
) -> Dict[str, float]:

  set1, set2 = await collect_cmp_vals(atype1, name1, atype2, name2, curs)

  return calc_dists(atype1, name1, set1, atype2, name2, set2, data_key)


async def collect_cmp_vals(atype1, name1, atype2, name2, curs):
  set1, set2 = Counter(), Counter()
  accum = {
    (atype1.value, name1): set1,
    (atype2.value, name2): set2, }
  get_key = itemgetter('atype', 'name')
  get_val = itemgetter('label', 'cnt')
  async for doc in curs:
    key = get_key(doc)
    label, cnt = get_val(doc)
    accum[key][label] += cnt
  return set1, set2


async def collect_cmp_vals_conts(atype1, name1, atype2, name2, curs):
  set1, set2 = Counter(), Counter()
  accum = {
    (atype1.value, name1): set1,
    (atype2.value, name2): set2, }
  conts1, conts2 = {}, {}
  conts = {
    (atype1.value, name1): conts1,
    (atype2.value, name2): conts2, }
  get_key = itemgetter('atype', 'name')
  get_val = itemgetter('label', 'conts')
  async for doc in curs:
    key = get_key(doc)
    label, cs = get_val(doc)
    cnt = len(cs)
    accum[key][label] += cnt
    conts[key][label] = cs
  return (set1, conts1), (set2, conts2)


def calc_dists(atype1, name1, cnts1, atype2, name2, cnts2, data_key):
  keys_union = tuple(sorted(cnts1.keys() | cnts2.keys()))
  if not keys_union:
    _logger.warning(
      'Нет данных %s для вычисления дистанцый между %s %s и %s %s', data_key,
      atype1, name1, atype2, name2)
    return dict(common=0, union=0, yaccard=0, jensen_shannon=1)

  cnt1 = sum(cnts1.values())
  if not cnts1:
    _logger.warning('Нет данных %s для вычисления дистанцый для %s %s',
      data_key, atype1, name1)
    return dict(common=0, union=0, yaccard=0, jensen_shannon=1)

  cnt2 = sum(cnts2.values())
  if not cnts2:
    _logger.warning('Нет данных %s для вычисления дистанцый для %s %s',
      data_key, atype2, name2)
    return dict(common=0, union=0, yaccard=0, jensen_shannon=1)

  # Йенсен-Шеннон расхождение
  uv1 = np.array(tuple(cnts1[k] / cnt1 for k in keys_union))
  uv2 = np.array(tuple(cnts2[k] / cnt2 for k in keys_union))
  # eps = 10e-15
  # uvm = (uv1 + uv2) / 2 + eps
  # uv1 += eps
  # uv2 += eps
  # KLD1 = np.sum(uv1 * np.log(uv1 / uvm))
  # KLD2 = np.sum(uv2 * np.log(uv2 / uvm))
  # JS = np.sqrt((KLD1 + KLD2) / 2)
  JS = jensenshannon(uv1, uv2)
  keys_intersect = cnts1.keys() & cnts2.keys()
  yaccard = len(keys_intersect) / len(keys_union)
  return dict(
    common=len(keys_intersect), union=len(keys_union), yaccard=yaccard,
    jensen_shannon=JS)


def get_cmp_authors_all(
  ngrmpr:NgrammParam, probability:float
) -> Dict[str, list]:
  pipelines = {}
  for fld_set in FieldsSet:
    # type: fld_set: FieldsSet
    pipeline = get_cmp_authors_ref_all(fld_set, ngrmpr, probability)
    pipelines[fld_set] = pipeline

  return pipelines


def get_cmp_authors_ref_all(
  field_col:FieldsSet, ngrmpr:NgrammParam, probability:float
) -> list:

  pipiline = []

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
  elif field_col in {FieldsSet.topic, FieldsSet.topic_strong}:
    pipiline += [
      {'$lookup': {
        'from': 'contexts', 'localField': '_id', 'foreignField': 'pubid',
        'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.topics'},
      {'$match': {'cont.topics.probability': {'$gte': probability}, }},
    ]
    if field_col == FieldsSet.topic_strong:
      pipiline += [
        {'$lookup': {
          'from': 'topics', 'localField': 'cont.topics._id',
          'foreignField': '_id', 'as': 'topic'}},
        {'$unwind': '$topic'},
        {'$unwind': {'path': '$uni_authors', 'preserveNullAndEmptyArrays': True}},
        {'$unwind': {'path': '$uni_cited', 'preserveNullAndEmptyArrays': True}},
        {'$unwind': {'path': '$uni_citing', 'preserveNullAndEmptyArrays': True}},
        {'$match': {
          '$or': [
            {'$expr': {'$eq': ['$topic.uni_author', {'$ifNull': ['$uni_authors', '']}]}},
            {'$expr': {'$eq': ['$topic.uni_cited', {'$ifNull': ['$uni_cited','']}]}},
            {'$expr': {'$eq': ['$topic.uni_citing', {'$ifNull': ['$uni_citing','']}]}}, ]}},
      ]
    pipiline += [
      {'$addFields': {'label': {'$split': ['$cont.topics.title', ', ']}}},
      {'$unwind': '$label'},
    ]

  pipiline += [
    {'$group': {
      '_id': {
        'author': '$uni_authors', 'cited': '$uni_cited',
        'citing': '$uni_citing', 'label': '$label'},
      'cnt': {'$sum': 1}}},
    {'$project': {
      '_id': 0, 'author': '$_id.author', 'cited': '$_id.cited',
      'citing': '$_id.citing', 'label': '$_id.label', 'cnt': '$cnt'}},
  ]

  return pipiline


async def calc_cmp_vals_all(curs, data_key) -> List[Dict[str, float]]:

  accum = defaultdict(Counter)
  get_val = itemgetter('label', 'cnt')
  async for doc in curs:
    label, cnt = get_val(doc)
    keys = {}
    keys.update(author=doc.get('author', ()))
    keys.update(cited=doc.get('cited', ()))
    keys.update(citing=doc.get('citing', ()))
    for atype, names in keys.items():
      if isinstance(names, str):
        cnts = accum[(names, atype)]
        cnts[label] += cnt
      else:
        for name in names:
          cnts = accum[(name, atype)]
          cnts[label] += cnt

  out = []
  _logger.debug('%s authors: %s', data_key, sorted(accum.keys()))
  for i, ((name1, atype1), (name2, atype2)) in enumerate(
    combinations(sorted(accum.keys()), 2),
    1
  ):
    cnts1 = accum[(name1, atype1)]
    cnts2 = accum[(name2, atype2)]
    vals = calc_dists(atype1, name1, cnts1, atype2, name2, cnts2, data_key)
    _logger.debug(
      '%s %d: %s %s -> %s %s: %s',
      data_key, i, name1, atype1, name2, atype2, vals)
    out.append(
      dict(
        author1=dict(atype=atype1, name=name1),
        author2=dict(atype=atype2, name=name2),
        vals=vals))

  return out

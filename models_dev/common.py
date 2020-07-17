#! /usr/bin/env python3
# -*- codong: utf-8 -*-
from typing import List, Dict, Optional

from models_dev.models import AuthorParam, NgrammParam


def filter_by_pubs_acc(authParams: AuthorParam) -> List[dict]:
  if authParams.is_empty():
    return []
  match = filter_acc_dict(authParams)
  pipeline = [
    {'$lookup': {
      'from': 'publications', 'localField': 'pubid', 'foreignField': '_id',
      'as': 'pub'}},
    {'$unwind': '$pub'},
    # {'$match': {'pub.uni_authors': {'$exists': 1}}},
    {'$match': {f'pub.{key}': val for key, val in match.items()}},
  ]
  return pipeline


def filter_acc_dict(ap: AuthorParam) -> Dict[str, str]:
  """Фильтр по author, cited, citing"""
  if ap.is_empty():
    return {}
  match = {
    f'uni_{key}': val for key, val in
    (('authors', ap.author), ('cited', ap.cited), ('citing', ap.citing))
    if val}
  return match


def get_ngramm_filter(
  np: NgrammParam, ngrm_field:Optional[str]=None
) -> Dict[str, dict]:
  if np.is_empty():
    return {}

  ltype_str:str = np.ltype.value if np.ltype is not None else ''

  if not ngrm_field:
    return {
      '$match': {
        f: v for f, v in (('nka', np.nka), ('type', ltype_str)) if v}}

  return {
    '$match': {
      f'{ngrm_field}.{f}': v
      for f, v in (('nka', np.nka), ('type', ltype_str)) if v}}


def _add_topic_pipeline(
  authorParams: AuthorParam, *, localField:str= 'topics._id', as_field:str= 'topic'
):
  pipeline = [
    {'$lookup': {
      'from': 'topics', 'localField': localField, 'foreignField': '_id',
      'as': as_field}},
    {"$unwind": "$" + as_field}, ]
  pipeline += filter_by_topic(authorParams)
  return pipeline


def filter_by_topic(
  ap: AuthorParam, *, as_field:str= 'topic'
):
  if ap.is_empty():
    return []
  pipeline = [
    {
      '$match': {
        "$or": [
          {f'{as_field}.uni_{fld}': val} for fld, val in
          (('author', ap.author), ('cited', ap.cited), ('citing', ap.citing),)
          if val]}}, ]
  return pipeline
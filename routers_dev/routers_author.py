# -*- codong: utf-8 -*-
from collections import Counter
from operator import itemgetter
from typing import Optional

from fastapi import APIRouter, Depends
from pymongo.collection import Collection
from sklearn.metrics import log_loss

from models_dev.db_authors import get_publics, get_cmp_authors
from models_dev.models import AType, NgrammParam, AuthorParam
from routers_dev.common import (
  DebugOption, Slot, depNgrammParamReq, depAuthorParamOnlyOne,
  depAuthorParamOnlyOne2)

router = APIRouter()


@router.get('/stat/',
  summary='Статистика по авторам', tags=['authors'])
async def _req_stat(
  atype:AType, ngrmpr:NgrammParam=Depends(depNgrammParamReq),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  # _logger.info('start func(%s)', id)
  pipeline = get_publics(atype, ngrmpr, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  coll:Collection = slot.mdb.publications
  curs = coll.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  out = []
  async for doc in curs:
    out.append(doc)

  return out


@router.get('/compare2authors/',
  summary='Сравнение 2х авторов', tags=['authors'])
async def _req_compare2authors(
  authorParams1:AuthorParam=Depends(depAuthorParamOnlyOne),
  authorParams2:AuthorParam=Depends(depAuthorParamOnlyOne2),
  _debug_option: Optional[DebugOption]=None,
  slot: Slot = Depends(Slot.req2slot)
):
  atype1, name1 = authorParams1.get_qual_auth()
  atype2, name2 = authorParams2.get_qual_auth()

  if authorParams1 == authorParams2:
    out = dict(
      author1=dict(atype=atype1, name=name1),
      author2=dict(atype=atype2, name=name2),
      yaccard=1
    )
    return out

  pipeline = get_cmp_authors(authorParams1, authorParams2)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  coll:Collection = slot.mdb.publications
  curs = coll.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  sets = [Counter(), Counter()]
  accum = {
    (atype1.value, name1): sets[0],
    (atype2.value, name2): sets[1],}

  get_key = itemgetter('atype', 'name')
  get_val = itemgetter('bundle', 'cnt')
  async for doc in curs:
    key = get_key(doc)
    bundle, cnt = get_val(doc)
    accum[key][bundle] += cnt

  keys_union = tuple(sorted(sets[0].keys() | sets[1].keys()))
  cnts1 = sum(sets[0].values())
  cnts2 = sum(sets[1].values())
  y_pred = tuple(
    (sets[0][k] / cnts1, sets[1][k] / cnts2) for k in keys_union)
  # y_pred = (
  #   tuple(sets[0][k] / cnts1 for k in keys_union),
  #   tuple(sets[1][k] / cnts2 for k in keys_union),
  # )
  ll = log_loss(
    keys_union, y_pred, labels=['author1', 'author2'])

  keys_intersect = sets[0].keys() & sets[1].keys()
  out = dict(
    author1=dict(atype=atype1, name=name1),
    author2=dict(atype=atype2, name=name2),
    yaccard=len(keys_intersect) / len(keys_union),
    log_loss=ll
  )
  return out

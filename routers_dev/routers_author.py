# -*- codong: utf-8 -*-
from collections import Counter
from operator import itemgetter
from typing import Dict, Optional

from fastapi import APIRouter, Depends
import numpy as np
from pymongo.collection import Collection
from scipy.spatial.distance import jensenshannon

from models_dev.db_authors import FieldsSet, get_publics, get_cmp_authors
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
  ngrmpr:NgrammParam=Depends(depNgrammParamReq),
  probability:Optional[float]=.5,
  _debug_option: Optional[DebugOption]=None,
  slot: Slot = Depends(Slot.req2slot)
):
  atype1, name1 = authorParams1.get_qual_auth()
  atype2, name2 = authorParams2.get_qual_auth()

  if authorParams1 == authorParams2:
    out = dict(
      author1=dict(atype=atype1, name=name1),
      author2=dict(atype=atype2, name=name2),
      **{k: dict(yaccard=1, jensen_shannon=0) for k in FieldsSet}
    )
    return out

  pipelines = get_cmp_authors(
    authorParams1, authorParams2, ngrmpr, probability)
  if _debug_option == DebugOption.pipeline:
    return pipelines

  coll:Collection = slot.mdb.publications

  if _debug_option == DebugOption.raw_out:
    out = {}
    for key, pipeline in pipelines.items():
      curs = coll.aggregate(pipeline)
      out_lst = [doc async for doc in curs]
      out[key] = out_lst
    return out

  vals = {}
  for key, pipeline in pipelines.items():
    curs = coll.aggregate(pipeline)
    calc_vals = await calc_cmp_vals(atype1, atype2, name1, name2, curs)
    vals[key] = calc_vals

  out = dict(
    author1=dict(atype=atype1, name=name1),
    author2=dict(atype=atype2, name=name2),
    **vals
  )
  return out


async def calc_cmp_vals(
  atype1:AType, atype2:AType, name1:str, name2:str, curs
) -> Dict[str, float]:

  sets = [Counter(), Counter()]
  accum = {
    (atype1.value, name1): sets[0],
    (atype2.value, name2): sets[1], }
  get_key = itemgetter('atype', 'name')
  get_val = itemgetter('label', 'cnt')
  async for doc in curs:
    key = get_key(doc)
    label, cnt = get_val(doc)
    accum[key][label] += cnt
  keys_union = tuple(sorted(sets[0].keys() | sets[1].keys()))
  cnts1 = sum(sets[0].values())
  cnts2 = sum(sets[1].values())
  # Йенсен-Шеннон расхождение
  eps = 10e-15
  uv1 = np.array(tuple(sets[0][k] / cnts1 for k in keys_union))
  uv2 = np.array(tuple(sets[1][k] / cnts2 for k in keys_union))
  # uvm = (uv1 + uv2) / 2 + eps
  # uv1 += eps
  # uv2 += eps
  # KLD1 = np.sum(uv1 * np.log(uv1 / uvm))
  # KLD2 = np.sum(uv2 * np.log(uv2 / uvm))
  # JS = np.sqrt((KLD1 + KLD2) / 2)
  JS = jensenshannon(uv1, uv2)

  keys_intersect = sets[0].keys() & sets[1].keys()
  yaccard = len(keys_intersect) / len(keys_union)
  return dict(yaccard=yaccard, jensen_shannon=JS)

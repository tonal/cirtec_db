# -*- codong: utf-8 -*-
from operator import itemgetter
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pymongo.collection import Collection

from models_dev.db_authors import (
  collect_cmp_vals, collect_cmp_vals_conts, FieldsSet, calc_cmp_vals,
  calc_cmp_vals_all, get_cmp_authors_all, get_cmp_authors_cont, get_publics,
  get_cmp_authors)
from models_dev.models import AType, NgrammParam, AuthorParam
from routers_dev.common import (
  DebugOption, Slot, depNgrammParamReq, depAuthorParamOnlyOne,
  depAuthorParamOnlyOne2)
from utils import get_logger_dev as get_logger


_logger = get_logger()

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


@router.get('/common2authors/',
  summary='Общие слова 2х авторов', tags=['authors'])
async def _req_common2authors(
  authorParams1:AuthorParam=Depends(depAuthorParamOnlyOne),
  authorParams2:AuthorParam=Depends(depAuthorParamOnlyOne2),
  ngrmpr:NgrammParam=Depends(depNgrammParamReq),
  probability:Optional[float]=.5,
  _debug_option: Optional[DebugOption]=None,
  slot: Slot = Depends(Slot.req2slot)
):
  pipelines = get_cmp_authors(
    authorParams1, authorParams2, ngrmpr, probability)
  if _debug_option == DebugOption.pipeline:
    return pipelines

  atype1, name1 = authorParams1.get_qual_auth()
  atype2, name2 = authorParams2.get_qual_auth()

  def_vals = dict(common=1, union=1)

  if authorParams1 == authorParams2:
    out = dict(
      author1=dict(atype=atype1, name=name1),
      author2=dict(atype=atype2, name=name2),
      **{k: def_vals for k in FieldsSet}
    )
    return out

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
    cnts1, cnts2 = await collect_cmp_vals(atype1, name1, atype2, name2, curs)
    keys_union = cnts1.keys() | cnts2.keys()
    keys_intersect = cnts1.keys() & cnts2.keys()
    words = sorted((w, cnts1[w], cnts2[w]) for w in keys_intersect)
    if key == FieldsSet.ngram:
      len_pref = len(ngrmpr.ltype.value) + 1
      words = ((w[len_pref:], c1, c2) for w, c1, c2 in words)
    common_words = [dict(word=w, author1=c1, author2=c2) for w, c1, c2 in words]
    vals[key] = dict(
      common=len(keys_intersect), union=len(keys_union),
      common_words=common_words
      )

  out = dict(
    author1=dict(atype=atype1, name=name1),
    author2=dict(atype=atype2, name=name2),
    **vals
  )
  return out


@router.get('/common2authors/bundle',
  summary='Общие bundle 2х авторов', tags=['authors'])
async def _req_common2authors_bundle(
  authorParams1:AuthorParam=Depends(depAuthorParamOnlyOne),
  authorParams2:AuthorParam=Depends(depAuthorParamOnlyOne2),
  word:str=Query(None, min_length=2),
  _debug_option: Optional[DebugOption]=None,
  slot: Slot = Depends(Slot.req2slot)
):
  out = await _req_common2authors_field(
    FieldsSet.bundle, authorParams1, authorParams2, word,
    slot=slot, _debug_option=_debug_option)
  return out


async def _req_common2authors_field(
  field:FieldsSet, authorParams1:AuthorParam, authorParams2:AuthorParam,
  word:Optional[str],
  *, ngrmpr:Optional[NgrammParam]=None, probability:Optional[float]=None,
  slot:Slot, _debug_option:Optional[DebugOption]=None,
):

  pipeline = get_cmp_authors_cont(
    authorParams1, authorParams2, word, field, ngrmpr, probability)

  if _debug_option == DebugOption.pipeline:
    return pipeline

  coll:Collection = slot.mdb.publications

  curs = coll.aggregate(pipeline, allowDiskUse=True)
  if _debug_option == DebugOption.raw_out:
    out = [doc async for doc in curs]
    return out

  atype1, name1 = authorParams1.get_qual_auth()
  atype2, name2 = authorParams2.get_qual_auth()

  (set1, conts1), (set2, conts2) = await collect_cmp_vals_conts(
    atype1, name1, atype2, name2, curs)
  keys_union = set1.keys() | set2.keys()
  keys_intersect = set1.keys() & set2.keys()
  words = sorted((w, set1[w], set2[w]) for w in keys_intersect)
  len_pref = len(ngrmpr.ltype.value) + 1 if field == FieldsSet.ngram else 0
  common_words = [
    dict(
      word=w[len_pref:],
      author1=dict(cnt=c1, conts=sorted(conts1.get(w, ()))),
      author2=dict(cnt=c2, conts=sorted(conts2.get(w, ()))))
    for w, c1, c2 in words]

  out = dict(
    author1=dict(atype=atype1, name=name1),
    author2=dict(atype=atype2, name=name2),
    common=len(keys_intersect), union=len(keys_union),
    common_words=common_words
    )
  return out


@router.get('/common2authors/ngram',
  summary='Общие ngram 2х авторов', tags=['authors'])
async def _req_common2authors_ngram(
  authorParams1:AuthorParam=Depends(depAuthorParamOnlyOne),
  authorParams2:AuthorParam=Depends(depAuthorParamOnlyOne2),
  word:str=Query(None, min_length=2),
  ngrmpr:NgrammParam=Depends(depNgrammParamReq),
  _debug_option: Optional[DebugOption]=None,
  slot: Slot = Depends(Slot.req2slot)
):
  out = await _req_common2authors_field(
    FieldsSet.ngram, authorParams1, authorParams2, word, ngrmpr=ngrmpr,
    slot=slot, _debug_option=_debug_option)
  return out


@router.get('/common2authors/ref_author',
  summary='Общие ref_author 2х авторов', tags=['authors'])
async def _req_common2authors_ref_author(
  authorParams1:AuthorParam=Depends(depAuthorParamOnlyOne),
  authorParams2:AuthorParam=Depends(depAuthorParamOnlyOne2),
  word:str=Query(None, min_length=2),
  _debug_option: Optional[DebugOption]=None,
  slot: Slot = Depends(Slot.req2slot)
):
  out = await _req_common2authors_field(
    FieldsSet.ref_author, authorParams1, authorParams2, word,
    slot=slot, _debug_option=_debug_option)
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
  pipelines = get_cmp_authors(
    authorParams1, authorParams2, ngrmpr, probability)
  if _debug_option == DebugOption.pipeline:
    return pipelines

  atype1, name1 = authorParams1.get_qual_auth()
  atype2, name2 = authorParams2.get_qual_auth()

  def_vals = dict(common=1, union=1, yaccard=1, jensen_shannon=0)

  if authorParams1 == authorParams2:
    out = dict(
      author1=dict(atype=atype1, name=name1),
      author2=dict(atype=atype2, name=name2),
      **{k: def_vals for k in FieldsSet}
    )
    return out

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
    calc_vals = await calc_cmp_vals(atype1, name1, atype2, name2, curs, key)
    vals[key] = calc_vals

  out = dict(
    author1=dict(atype=atype1, name=name1),
    author2=dict(atype=atype2, name=name2),
    **vals
  )
  return out


@router.get('/compare_all_authors/',
  summary='Сравнение всех авторов', tags=['authors'])
async def _req_compare_authors_all(
  ngrmpr: NgrammParam = Depends(depNgrammParamReq),
  probability: Optional[float] = .5,
  _debug_option: Optional[DebugOption] = None,
  slot: Slot = Depends(Slot.req2slot)
):
  pipelines = get_cmp_authors_all(ngrmpr, probability)
  if _debug_option == DebugOption.pipeline:
    return pipelines

  def_vals = dict(yaccard=1, jensen_shannon=0)

  coll:Collection = slot.mdb.publications

  if _debug_option == DebugOption.raw_out:
    out = {}
    for key, pipeline in pipelines.items():
      if pipeline:
        curs = coll.aggregate(pipeline)
        out_lst = [doc async for doc in curs]
        out[key] = out_lst
      else:
        out[key] = []
    return out

  out_dict = {}
  get_authors = itemgetter('author1', 'author2')
  get_key = itemgetter('atype', 'name')
  get_vals = itemgetter('vals')
  for key, pipeline in pipelines.items():
    if not pipeline:
      continue

    curs = coll.aggregate(pipeline)
    calc_vals = await calc_cmp_vals_all(curs, key)
    for doc in calc_vals:
      author1, author2 = get_authors(doc)
      atype1, name1 = get_key(author1)
      atype2, name2 = get_key(author2)
      vals = get_vals(doc)
      ovals = out_dict.setdefault((name1, atype1, name2, atype2), {})
      ovals[key] = vals

  out = [
    dict(
      author1=dict(atype=atype1, name=name1),
      author2=dict(atype=atype2, name=name2),
      **vals)
    for (name1, atype1, name2, atype2), vals in sorted(out_dict.items())]
  return out

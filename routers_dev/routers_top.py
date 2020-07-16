#! /usr/bin/env python3
# -*- codong: utf-8 -*-
from collections import Counter
from operator import itemgetter
from typing import Optional

from fastapi import APIRouter, Depends
from pymongo.collection import Collection

from routers_dev.common import DebugOption, Slot, depNgrammParam
from models_dev.dbquery import (
  AuthorParam, NgrammParam, get_refauthors_pipeline, get_refbindles_pipeline,
  get_top_cocitauthors_pipeline, get_top_cocitauthors_publications_pipeline,
  get_top_cocitrefs2_pipeline, get_top_cocitrefs_pipeline,
  get_top_ngramms_pipeline, get_top_ngramms_publications_pipeline,
  get_top_topics_pipeline, get_top_topics_publications_pipeline)


router = APIRouter()


@router.get('/cocitauthors/',
  summary='Топ N со-цитируемых авторов')
async def _req_top_cocitauthors(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_cocitauthors_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  coll: Collection = slot.mdb.contexts
  curs = coll.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    out = [doc async for doc in curs]
    return out

  out = []
  async for doc in curs:
    title = doc.pop('_id')
    out.append(dict(title=title, **doc))
  return out


@router.get('/cocitrefs/',
  summary='Топ N со-цитируемых референсов')
async def _req_top_cocitrefs(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_cocitrefs_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  coll: Collection = slot.mdb.contexts

  def repack(_id, count, conts, bundles):
    authors = bundles.get('authors')
    title = bundles.get('title')
    year = bundles.get('year', '?')
    descr = f'{" ".join(authors) if authors else "?"} ({year})'
    if title:
      descr += f' {title}'
    return dict(bundle=_id, descr=descr, intxtids=conts)

  curs = coll.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  out = [repack(**doc) async for doc in curs]

  return out


@router.get('/cocitauthors/publications/',
  summary='Топ N со-цитируемых авторов по публикациям')
async def _req_top_cocitauthors_pubs(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_cocitauthors_publications_pipeline(
    topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out


@router.get('/cocitrefs/cocitrefs/',
  summary='Топ N со-цитируемых авторов по публикациям')
async def _req_top_cocitrefs2(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_cocitrefs2_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  if _debug_option == DebugOption.raw_out:
    out = [row async for row in contexts.aggregate(pipeline)]
    return out

  out = []
  async for row in contexts.aggregate(pipeline):
    row["frags"] = Counter(sorted(row["frags"]))
    out.append(row)
  return out


@router.get('/ngramms/',
  summary='Топ N фраз по публикациям')
async def _req_top_ngramms(
  topn:Optional[int]=10,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_ngramms_pipeline(topn, authorParams, ngrammParam)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  coll: Collection = slot.mdb.contexts
  curs = coll.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  get_as_tuple = itemgetter('_id', 'count', 'count_cont', 'conts')
  get_pubs = itemgetter('cont_id', 'cnt')
  key_sort = lambda kv: (-kv[-1], kv[0])
  get_name = itemgetter('title')
  get_ltype = itemgetter('type')

  out = []
  async for doc in curs:
    nid, cnt, count_cont, conts = get_as_tuple(doc)
    odoc = dict(
      title=get_name(nid), type=get_ltype(nid),
      all=cnt, count_cont=count_cont,
      contects=dict(
        sorted(
          Counter(
            p for p, n in (get_pubs(co) for co in conts)
            for _ in range(n)
          ).most_common(),
          key=key_sort)))
    out.append(odoc)
  return out


@router.get('/ngramms/publications/',
  summary='Топ N фраз по публикациям')
async def _req_top_ngramm_pubs(
  topn:Optional[int]=10,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_ngramms_publications_pipeline(
    topn, authorParams, ngrammParam)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out


@router.get('/ref_authors/',
  summary='Топ N авторов бандлов')
async def _req_top_ref_authors(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_refauthors_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  coll: Collection = slot.mdb.contexts
  curs = coll.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]
  out = []
  async for doc in curs:
    doc.pop('pos_neg', None)
    doc.pop('frags', None)
    out.append(doc)
  return out


@router.get('/top/ref_bundles/',
  summary='Топ N бандлов')
async def _req_top_ref_bundles(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_refbindles_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  coll: Collection = slot.mdb.contexts
  curs = coll.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  out = []
  async for doc in curs:
    doc.pop('pos_neg', None)
    doc.pop('frags', None)
    if 'authors' not in doc:
      doc['authors'] = []
    out.append(doc)

  return out


@router.get('/top/topics/',
  summary='Топ N топиков')
async def _req_top_topics(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_topics_pipeline(topn, authorParams, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  coll: Collection = slot.mdb.contexts
  out = [doc async for doc in coll.aggregate(pipeline)]
  return out


@router.get('/top/topics/publications/',
  summary='Топ N топиков')
async def _req_top_topics_pubs(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_topics_publications_pipeline(
    topn, authorParams, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out

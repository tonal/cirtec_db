#! /usr/bin/env python3
# -*- codong: utf-8 -*-
from typing import Optional

from fastapi import APIRouter, Depends
from pymongo.collection import Collection

from server_common_devf import DebugOption, Slot, depNgrammParam
from server_dbquery_dev import (
  AuthorParam, NgrammParam, get_pos_neg_cocitauthors_pipeline,
  get_pos_neg_contexts_pipeline, get_pos_neg_ngramms_pipeline,
  get_pos_neg_pubs_pipeline, get_pos_neg_topics_pipeline,
  get_refauthors_pipeline, get_refbindles_pipeline)


router = APIRouter()


@router.get('/cocitauthors/',
  summary='для каждого класса тональности привести топ со-цитируемых авторов')
async def _req_pos_neg_cocitauthors(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option: DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_pos_neg_cocitauthors_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]
  return out


@router.get('/contexts/',
  summary='для каждого класса тональности показать общее количество контекстов')
async def _req_pos_neg_contexts(
  authorParams:AuthorParam=Depends(),
  _debug_option: DebugOption = None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_pos_neg_contexts_pipeline(authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]
  return out


@router.get('/ngramms/',
  summary='для каждого класса тональности показать топ фраз с количеством повторов каждой')
async def _req_pos_neg_ngramms(
  topn:Optional[int]=10,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  _debug_option: DebugOption =None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_pos_neg_ngramms_pipeline(
    topn, authorParams, ngrammParam)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]
  return out


@router.get('/pubs/',) # summary='Топ N со-цитируемых референсов')
async def _req_pos_neg_pubs(
  authorParams:AuthorParam=Depends(),
  _debug_option: DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_pos_neg_pubs_pipeline(authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts: Collection = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]
  out = []
  async for doc in curs:
    pid: str = doc['_id']
    name: str = doc['pub']['name']
    classif = doc['pos_neg']
    neutral = sum(v['cnt'] for v in classif if v['val'] == 0)
    positive = sum(v['cnt'] for v in classif if v['val'] > 0)
    negative = sum(v['cnt'] for v in classif if v['val'] < 0)
    out.append(
      dict(
        pub=pid, name=name, neutral=int(neutral), positive=int(positive),
        negative=int(negative)))
  return out


@router.get('/ref_authors/',) # summary='Топ N со-цитируемых референсов')
async def _req_pos_neg_refauthors(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option: DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_refauthors_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts:Collection = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  out = []
  async for row in curs:
    row.pop('frags', None)
    classify = row.pop('pos_neg', None)
    if classify:
      neutral = sum(1 for v in classify if v['val'] == 0)
      positive = sum(1 for v in classify if v['val'] > 0)
      negative = sum(1 for v in classify if v['val'] < 0)
      row.update(
        class_pos_neg=dict(
          neutral=neutral, positive=positive, negative=negative))
    out.append(row)
  return out


@router.get('/ref_bundles/',) # summary='Топ N со-цитируемых референсов')
async def _req_pos_neg_refbundles(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option: DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_refbindles_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts:Collection = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]
  out = []
  async for doc in curs:
    doc.pop('frags', None)
    classify = doc.pop('pos_neg', None)
    if classify:
      neutral = sum(1 for v in classify if v['val'] == 0)
      positive = sum(1 for v in classify if v['val'] > 0)
      negative = sum(1 for v in classify if v['val'] < 0)
      doc.update(
        class_pos_neg=dict(
          neutral=neutral, positive=positive, negative=negative))
    out.append(doc)
  return out


@router.get('/topics/',
  summary='для каждого класса тональности показать топ топиков с количеством')
async def _req_pos_neg_topics(
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.5,
  _debug_option: DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_pos_neg_topics_pipeline(authorParams, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]
  return out

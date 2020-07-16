#! /usr/bin/env python3
# -*- codong: utf-8 -*-
from collections import defaultdict
from functools import partial
from operator import itemgetter
from typing import Optional

from fastapi import APIRouter, Depends
from pymongo import ASCENDING
from pymongo.collection import Collection

from routers_dev.common import DebugOption, Slot, depNgrammParam
from models_dev.dbquery import (
  AuthorParam, NgrammParam, filter_acc_dict,
  get_frags_ngramms_ngramms_branch_pipeline,
  get_frags_ngramms_ngramms_branch_root, get_publications_cocitauthors_pipeline,
  get_publications_ngramms_pipeline, get_publications_topics_topics_pipeline,
  get_refauthors_part_pipeline)
from server_utils import to_out_typed

router = APIRouter()


@router.get('/ngramms/ngramms/',
  summary='Кросс-распределение «публикации» - «фразы из контекстов цитирований»')
async def _req_publ_ngramm_ngramm(
  topn:Optional[int]=10,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  topn_ngramm:Optional[int]=10,
  _debug_option:DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline_root = get_frags_ngramms_ngramms_branch_root(
    topn, authorParams, ngrammParam)
  pipeline_branch = get_frags_ngramms_ngramms_branch_pipeline(ngrammParam)
  if _debug_option == DebugOption.pipeline:
    return dict(pipeline_root=pipeline_root, pipeline_branch=pipeline_branch)

  ngrm2tuple = itemgetter('_id', 'title', 'type', 'nka', 'count', 'conts')
  contexts = slot.mdb.contexts
  topN = tuple(
    [ngrm2tuple(doc) async for doc in contexts.aggregate(pipeline_root)])
  exists = frozenset(map(itemgetter(0), topN))

  out_list = []

  for i, (ngrmm, title, typ_, nka, cnt, conts) in enumerate(topN, 1):
    congr = defaultdict(set)
    titles = {}
    types = {}

    work_pipeline = [{
      '$match': {
        'frag_num': {'$gt': 0}, '_id': {'$in': conts}}}] + pipeline_branch + [
                      {'$match': {'cont.type': typ_}}]
    # _logger.debug('ngrmm: "%s", cnt: %s, pipeline_branch: %s', ngrmm, cnt, work_pipeline)
    # print('ngrmm: "%s", cnt: %s, pipeline_branch: %s' % (ngrmm, cnt, work_pipeline))

    async for doc in contexts.aggregate(work_pipeline):
      cont = doc['cont']
      ngr_id = cont['_id']
      if ngr_id not in exists:
        continue
      pubid = doc['pubid']
      congr[ngr_id].add(pubid)
      titles[ngr_id] = cont['title']
      types[ngr_id] = cont['type']

    pubids = congr.pop(ngrmm)
    crossgrams = []

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-len(kv[1]), kv[0])), 1):
      crossgrams.append(
        dict(
          title=titles[co], type=types[co], pubids=sorted(cnts), cnt=len(cnts)))
    if topn_ngramm:
      crossgrams = crossgrams[:topn_ngramm]

    out_list.append(
      dict(
        title=titles[ngrmm], type=typ_, nka=nka, cnt_pubs=len(pubids),
        cnt_cross=len(congr), pubids=pubids, crossgrams=crossgrams))

  return out_list


@router.get('/publications/',
  summary='Публикации')
async def _req_publications(
  authorParams:AuthorParam=Depends(),
  _debug_option:DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  query = {
    'name': {'$exists': 1},
    **filter_acc_dict(authorParams)}
  if _debug_option == DebugOption.pipeline:
    return dict(query=query)

  to_out = partial(to_out_typed, type='publication')

  publications = slot.mdb.publications
  curs = publications.find(query).sort([('year', ASCENDING), ('_id', ASCENDING)])
  if _debug_option == DebugOption.raw_out:
    out = [doc async for doc in curs]
    return out

  out = [to_out(**doc) async for doc in curs]
  return out


@router.get('/publications/cocitauthors/',
  summary='Кросс-распределение «со-цитируемые авторы» по публикациям')
async def _req_publ_publications_cocitauthors(
  authorParams:AuthorParam=Depends(),
  topn_auth:Optional[int]=None,
  _debug_option:DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_publications_cocitauthors_pipeline(
    authorParams, topn_auth)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)

  out = [doc async for doc in curs]
  return out


@router.get('/publications/ngramms/',
  summary='Кросс-распределение «фразы из контекстов цитирований» по публикациям')
async def _req_publ_publications_ngramms(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  topn_gramm:Optional[int]=10,
  _debug_option:DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_publications_ngramms_pipeline(
    topn, authorParams, ngrammParam, topn_gramm)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)

  out = [doc async for doc in curs]
  return out


@router.get('/topics/topics/',
  summary='Кросс-распределение «публикации» - «топики контекстов цитирований»')
async def _req_publ_topics_topics(
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.4,
  _debug_option:DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_publications_topics_topics_pipeline(
    authorParams, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    out = [doc async for doc in curs]
    return out

  out = [doc async for doc in curs]
  return out


@router.get('/ref_authors/',) # summary='Топ N со-цитируемых референсов')
async def _req_pubs_refauthors(
  top_auth:Optional[int]=3,
  authorParams:AuthorParam=Depends(),
  _debug_option:DebugOption=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_refauthors_part_pipeline(top_auth, AuthorParam())
  if _debug_option == DebugOption.pipeline:
    return pipeline

  publications: Collection = slot.mdb.publications
  contexts: Collection = slot.mdb.contexts

  out = []
  async for pub in publications.find(
    # {'uni_authors': 'Sergey-Sinelnikov-Murylev'},
    {'name': {'$exists': 1}, **filter_acc_dict(authorParams),},
    projection={'_id': True, 'name': True}, sort=[('_id', ASCENDING)]
  ):
    pid = pub['_id']
    pub_pipeline = [{'$match': {'pubid': pid}}] + pipeline
    ref_authors = []
    async for row in contexts.aggregate(pub_pipeline):
      row.pop('pos_neg', None)
      row.pop('frags', None)
      ref_authors.append(row)

    pub_out = dict(pubid=pid, name=pub['name'], ref_authors=ref_authors)
    out.append(pub_out)

  return out

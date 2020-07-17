# -*- codong: utf-8 -*-
from itertools import chain, groupby, islice
from operator import itemgetter
from typing import Optional

from fastapi import APIRouter, Depends
from pymongo.collection import Collection

from routers_dev.common import DebugOption, Slot
from models_dev.dbquery import (
  get_ref_auth4ngramm_tops_pipeline,
  get_ref_bund4ngramm_tops_pipeline, get_refauthors_part_pipeline,
  get_top_detail_bund_refauthors)
from models_dev.models import AuthorParam

router = APIRouter()


@router.get('/ref_auth4ngramm_tops/',) # summary='Топ N со-цитируемых референсов')
async def _ref_auth4ngramm_tops(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_ref_auth4ngramm_tops_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  curs = contexts.aggregate(pipeline, allowDiskUse=True)
  if _debug_option == DebugOption.raw_out:
    return [cont async for cont in curs]

  out_bund = []
  get_topics = lambda c: c.get('topics', ())
  get_topic = itemgetter('title', 'probability')
  get_first = itemgetter(0)
  get_second = itemgetter(1)
  if probability is None:
    probability = .5

  def topic_stat(it_tp):
    it_tp = tuple(it_tp)
    # probabs = tuple(map(get_second, it_tp))
    return dict(count=len(it_tp), )  # probability_avg=mean(probabs),  # probability_pstdev=pstdev(probabs))

  get_count = itemgetter('count')
  get_ngr = itemgetter('_id', 'cnt')
  async for cont in curs:
    conts = cont.pop('conts')

    cont_ids = map(itemgetter('cid'), conts)

    topics = chain.from_iterable(map(get_topics, conts))
    # удалять топики < 0.5
    topics = ((t, p) for t, p in map(get_topic, topics) if p >= probability)
    topics = (dict(topic=t, **topic_stat(it_tp)) for t, it_tp in
    groupby(sorted(topics, key=get_first), key=get_first))
    topics = sorted(topics, key=get_count, reverse=True)

    get_ngrs = lambda cont: cont.get('ngrams') or ()
    ngrams = chain.from_iterable(map(get_ngrs, conts))
    # только 2-grams и lemmas
    ngrams = ((n.split('_', 1)[-1].split(), c) for n, c in map(get_ngr, ngrams)
    if n.startswith('lemmas_'))
    ngrams = ((' '.join(n), c) for n, c in ngrams if len(n) == 2)
    ngrams = (dict(ngramm=n, count=sum(map(get_second, it_nc))) for n, it_nc in
    groupby(sorted(ngrams, key=get_first), key=get_first))
    ngrams = sorted(ngrams, key=get_count, reverse=True)
    ngrams = islice(ngrams, 10)
    cont.update(cont_ids=tuple(cont_ids), topics=tuple(topics),
      ngrams=tuple(ngrams))
    out_bund.append(cont)

  return out_bund


@router.get('/ref_bund4ngramm_tops/',) # summary='Топ N со-цитируемых референсов')
async def _req_bund4ngramm_tops(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  probability: Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_ref_bund4ngramm_tops_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [cont async for cont in curs]

  out = []

  get_probab = itemgetter('probability')
  get_first = itemgetter(0)
  get_second = itemgetter(1)
  get_topic = itemgetter('title', 'probability')

  def topic_stat(it_tp):
    it_tp = tuple(it_tp)
    probabs = tuple(map(get_second, it_tp))
    return dict(count=len(
      it_tp), )  # probability_avg=mean(probabs),  # probability_pstdev=pstdev(probabs))

  get_topics = lambda cont: cont.get('topics') or ()
  get_count = itemgetter('count')
  get_ngrs = lambda cont: cont.get('ngrams') or ()
  get_ngr = itemgetter('_id', 'cnt')
  async for cont in curs:
    conts = cont.pop('conts')

    cont_ids = map(itemgetter('cid'), conts)

    topics = chain.from_iterable(map(get_topics, conts))
    # удалять топики < 0.5
    topics = ((t, p) for t, p in map(get_topic, topics) if p >= probability)
    topics = (dict(topic=t, **topic_stat(it_tp)) for t, it_tp in
    groupby(sorted(topics, key=get_first), key=get_first))
    topics = sorted(topics, key=get_count, reverse=True)

    ngrams = chain.from_iterable(map(get_ngrs, conts))
    # только 2-grams и lemmas
    ngrams = ((n.split('_', 1)[-1].split(), c) for n, c in map(get_ngr, ngrams)
    if n.startswith('lemmas_'))
    ngrams = ((' '.join(n), c) for n, c in ngrams if len(n) == 2)
    ngrams = (dict(ngramm=n, count=sum(map(get_second, it_nc))) for n, it_nc in
    groupby(sorted(ngrams, key=get_first), key=get_first))
    ngrams = sorted(ngrams, key=get_count, reverse=True)
    ngrams = islice(ngrams, 10)
    cont.update(cont_ids=tuple(cont_ids), topics=tuple(topics),
      ngrams=tuple(ngrams))
    out.append(cont)

  return out


@router.get('/by_frags/ref_authors/',) # summary='Топ N со-цитируемых референсов')
async def _req_by_frags_refauthors(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_refauthors_part_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  out = []
  for fnum in range(1, 6):
    out_frag = []
    work_pipe = [
      {'$match': {'frag_num': fnum}}
    ] + pipeline
    async for row in contexts.aggregate(work_pipe):
      row.pop('pos_neg', None)
      row.pop('frags', None)
      out_frag.append(row)

    out.append(dict(frag_num=fnum, refauthors=out_frag))

  return out


@router.get('/top_detail_bund/ref_authors/',) # summary='Топ N со-цитируемых референсов')
async def _req_top_detail_bund_refauthors(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_detail_bund_refauthors(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts:Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out

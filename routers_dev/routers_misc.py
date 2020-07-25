# -*- codong: utf-8 -*-
from collections import Counter
from itertools import chain, groupby, islice
from operator import itemgetter
from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.params import Query
from pymongo.collection import Collection

from routers_dev.common import DebugOption, Slot, depNgrammParamReq
from models_dev.db_pipelines import get_refauthors_part
from models_dev.db_misc import (
  get_ngramm_author_stat, get_ref_auth4ngramm_tops, get_ref_bund4ngramm_tops,
  get_top_detail_bund_refauthors)
from models_dev.models import AuthorParam, Authors, NgrammParam


router = APIRouter()


@router.get('/ngramm_author_stat/', summary='Статистика фраз по автору')
async def _ref_ngramm_author_stat(
  topn:Optional[int]=None,
  author:Authors=Query(...),
  ngrammParam:NgrammParam=Depends(depNgrammParamReq),
  _debug_option: Optional[DebugOption] = None,
  slot: Slot = Depends(Slot.req2slot)
):
  pipeline = get_ngramm_author_stat(topn, author, ngrammParam)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  publications: Collection = slot.mdb.publications
  curs = publications.aggregate(pipeline, allowDiskUse=True)

  if _debug_option == DebugOption.raw_out:
    return [cont async for cont in curs]

  out = []

  get_at = itemgetter('atype', 'cnt_tot')
  async for cont in curs:
    atypes = cont.pop('atypes')
    cnt = Counter()
    cnt_tot = Counter()
    for atc in atypes:
      ats, cnt_tots = get_at(atc)
      for at in ats:
        cnt[at] += 1
        cnt_tot[at] += cnt_tots
    cnt_all = cont.pop('cnt')
    # assert cnt_all == sum(cnt.values()), f'{cnt_all} != {sum(cnt.values())}: {cont}, {atypes}'
    cnt_all_tot = cont.pop('cnt_tot')
    # assert cnt_all_tot == sum(cnt_tot.values())
    cont['cnt'] = dict(
      all=cnt_all, **cnt,
      **{f'{k}_proc': round((v/cnt_all)*100, 3) for k, v in cnt.items()})
    cont['cnt_tot'] = dict(
      all=cnt_all_tot, **cnt_tot,
      **{f'{k}_proc': round((v/cnt_all_tot)*100, 3) for k, v in cnt_tot.items()})

    out.append(cont)

  return out


@router.get('/ref_auth4ngramm_tops/',) # summary='Топ N со-цитируемых референсов')
async def _ref_auth4ngramm_tops(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_ref_auth4ngramm_tops(topn, authorParams)
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
  pipeline = get_ref_bund4ngramm_tops(topn, authorParams)
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
  pipeline = get_refauthors_part(topn, authorParams)
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

# -*- codong: utf-8 -*-
from collections import Counter
from operator import itemgetter
from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.params import Query
from pymongo.collection import Collection

from routers_dev.common import DebugOption, Slot, depNgrammParamReq
from models_dev.db_pipelines import (
  get_refauthors, get_refbindles)
from models_dev.db_top import (
  get_top_cocitauthors, get_top_cocitauthors_publications, get_top_cocitrefs2,
  get_top_ngramm_author_stat, get_top_cocitrefs, get_top_ngramms,
  get_top_ngramms_publications, get_top_topics, get_top_topics_publications)
from models_dev.models import AuthorParam, Authors, NgrammParam

router = APIRouter()


@router.get('/cocitauthors/', tags=['top'],
  summary='Топ N со-цитируемых авторов')
async def _req_top_cocitauthors(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_cocitauthors(topn, authorParams)
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


@router.get('/cocitrefs/', tags=['top'],
  summary='Топ N со-цитируемых референсов')
async def _req_top_cocitrefs(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_cocitrefs(topn, authorParams)
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


@router.get('/cocitauthors/publications/', tags=['top'],
  summary='Топ N со-цитируемых авторов по публикациям')
async def _req_top_cocitauthors_pubs(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_cocitauthors_publications(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out


@router.get('/cocitrefs/cocitrefs/', tags=['top'],
  summary='Топ N со-цитируемых авторов по публикациям')
async def _req_top_cocitrefs2(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_cocitrefs2(topn, authorParams)
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


@router.get('/ngramms/', tags=['top'],
  summary='Топ N фраз по публикациям')
async def _req_top_ngramms(
  topn:Optional[int]=10,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_ngramms(topn, authorParams, ngrammParam)
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


@router.get('/ngramm/author_stat/', tags=['top'],
  summary='Статистика фраз по автору')
async def _ref_ngramm_author_stat(
  topn:Optional[int]=None,
  author:Authors=Query(...),
  ngrammParam:NgrammParam=Depends(depNgrammParamReq),
  _debug_option: Optional[DebugOption] = None,
  slot: Slot = Depends(Slot.req2slot)
):
  pipeline = get_top_ngramm_author_stat(topn, author, ngrammParam)
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


@router.get('/ngramms/publications/', tags=['top'],
  summary='Топ N фраз по публикациям')
async def _req_top_ngramm_pubs(
  topn:Optional[int]=10,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_ngramms_publications(topn, authorParams, ngrammParam)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out


@router.get('/ref_authors/', tags=['top'],
  summary='Топ N авторов бандлов')
async def _req_top_ref_authors(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_refauthors(topn, authorParams)
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


@router.get('/ref_bundles/', tags=['top'],
  summary='Топ N бандлов')
async def _req_top_ref_bundles(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_refbindles(topn, authorParams)
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


@router.get('/topics/', tags=['top'],
  summary='Топ N топиков')
async def _req_top_topics(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_topics(topn, authorParams, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  coll: Collection = slot.mdb.contexts
  out = [doc async for doc in coll.aggregate(pipeline)]
  return out


@router.get('/topics/publications/', tags=['top'],
  summary='Топ N топиков')
async def _req_top_topics_pubs(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_top_topics_publications(topn, authorParams, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out

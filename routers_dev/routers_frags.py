# -*- codong: utf-8 -*-
from collections import Counter, defaultdict
from functools import reduce
from itertools import groupby
from operator import itemgetter
from typing import Optional

from fastapi import APIRouter, Depends
from pymongo.collection import Collection

from routers_dev.common import DebugOption, Slot, depNgrammParam
from models_dev.dbquery import (
  get_frag_pos_neg_cocitauthors2,
  get_frag_pos_neg_contexts, get_frag_publications,
  get_frags_cocitauthors_cocitauthors_pipeline,
  get_frags_cocitauthors_ngramms_pipeline, get_frags_cocitauthors_pipeline,
  get_frags_cocitauthors_topics_pipeline,
  get_frags_ngramms_cocitauthors_pipeline,
  get_frags_ngramms_ngramms_branch_pipeline,
  get_frags_ngramms_ngramms_branch_root, get_frags_ngramms_pipeline,
  get_frags_ngramms_topics_pipeline, get_frags_topics_cocitauthors_pipeline,
  get_frags_topics_ngramms_pipeline, get_frags_topics_pipeline,
  get_frags_topics_topics_pipeline, get_refauthors_pipeline,
  get_refbindles_pipeline)
from models_dev.models import AuthorParam, NgrammParam

router = APIRouter()


@router.get('/cocitauthors/',
  summary='Распределение «со-цитируемые авторы» по 5-ти фрагментам')
async def _req_frags_cocitauthors(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_cocitauthors_pipeline(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  coll: Collection = slot.mdb.contexts
  curs = coll.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  out = []
  async for doc in curs:
    frags = Counter(doc['frags'])
    out_dict = dict(name=doc['_id'], count=doc['count'], frags=frags)
    out.append(out_dict)

  return out


@router.get('/cocitauthors/cocitauthors/',
  summary='Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»')
async def _req_frags_cocitauthors_cocitauthors(
  topn:Optional[int]=100,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_cocitauthors_cocitauthors_pipeline(
    topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline, allowDiskUse=True)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]
  out = []
  async for doc in curs:
    cocitpair = doc['cocitpair']
    conts = doc['conts']
    pubids = tuple(frozenset(map(itemgetter('pubid'), conts)))
    contids = tuple(map(itemgetter('cont_id'), conts))
    frags = Counter(map(itemgetter('frag_num'), conts))
    out.append(dict(
      cocitpair=tuple(cocitpair.values()),
      intxtid_cnt=len(contids), pub_cnt=len(pubids),
      frags=dict(sorted(frags.items())), pubids=pubids, intxtids=contids))
  return out


@router.get('/cocitauthors/ngramms/',
  summary='Кросс-распределение «со-цитирования» - «фразы из контекстов цитирований»')
async def _req_frags_cocitauthors_ngramms(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  topn_gramm:Optional[int]=10,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_cocitauthors_ngramms_pipeline(
    topn, authorParams, ngrammParam)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline, allowDiskUse=True)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]
  out = []
  get_frag_num = itemgetter('frag_num')
  get_fn_cnt = itemgetter('fn', 'cnt')
  key_first = itemgetter(0)
  key_last = itemgetter(-1)
  def ngr2tuple(d):
    ngrm = d['ngrm']
    ret = ngrm['title'], ngrm['type'], ngrm['nka'], d['count'], d['frags']
    return ret
  key_ngrm_sort = lambda v: (-v[-2], *v[:-1])
  async for doc in curs:
    cocit_author = doc['_id']
    conts = doc['conts']
    frags = Counter(map(get_frag_num, conts))
    crossgrams = tuple(
      dict(
        title=title, type=lt, nka=nka, count=cnt,
        frags=dict(
          (fn, sum(map(key_last, cnts)))
          for fn, cnts in groupby(sorted(map(get_fn_cnt, fr)), key=key_first)))
        for title, lt, nka, cnt, fr in sorted(
          map(ngr2tuple, doc['ngrms']), key=key_ngrm_sort))
    if topn_gramm:
      crossgrams = crossgrams[:topn_gramm]
    out.append(dict(
      cocit_author=cocit_author, count=doc['count'],
      frags=dict(sorted(frags.items())), crossgrams=crossgrams))

  return out


@router.get('/cocitauthors/topics/',
  summary='Кросс-распределение «со-цитирования» - «топики контекстов цитирований»')
async def _req_frags_cocitauthors_topics(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_cocitauthors_topics_pipeline(
    topn, authorParams, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  out = []
  get_frag_num = itemgetter('frag_num')
  get_fn = itemgetter('fn')
  topic2tuple = itemgetter('topic', 'count', 'frags')
  key_topic_sort = lambda t: (-t[1], t[0])
  async for doc in curs:
    cocit_author = doc['_id']
    conts = doc['conts']
    frags = Counter(map(get_frag_num, conts))
    crosstopics = tuple(
      dict(
        topic=topic, count=cnt,
        frags=Counter(sorted(map(get_fn, fr))))
        for topic, cnt, fr in sorted(
          map(topic2tuple, doc['topics']), key=key_topic_sort))
    out.append(dict(
      cocit_author=cocit_author, count=doc['count'],
      frags=dict(sorted(frags.items())), crosstopics=crosstopics))
  return out


@router.get('/ngramms/',
  summary='Распределение «5 фрагментов» - «фразы из контекстов цитирований»')
async def _req_frags_ngramms(
  topn:Optional[int]=10,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_ngramms_pipeline(topn, authorParams, ngrammParam)
  if _debug_oprion == DebugOption.pipeline:
    return pipeline
  coll: Collection = slot.mdb.contexts
  cursor = coll.aggregate(pipeline)
  if _debug_oprion == DebugOption.raw_out:
    return [doc async for doc in cursor]
  out = []
  async for doc in cursor:
    frags = dict(sorted(map(itemgetter('frag_num', 'count'), doc['frags'])))
    out_dict = dict(
      title=doc['title'], type=doc['type'], nka=doc['nka'], count=doc['count'],
      frags=frags)
    out.append(out_dict)

  return out


@router.get('/ngramms/cocitauthors/',
  summary='Кросс-распределение «фразы» - «со-цитирования»')
async def _req_frags_ngramms_cocitauthors(
  topn: Optional[int]=10,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  topn_cocitauthors: Optional[int]=None,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_ngramms_cocitauthors_pipeline(
    topn, authorParams, ngrammParam)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline, allowDiskUse=True)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]
  out = []
  get_frags = itemgetter('frags')
  get_fn_cnt = itemgetter('fn', 'cnt')
  key_first = itemgetter(0)
  key_last = itemgetter(-1)
  auth2tuple = itemgetter('auth', 'count', "frags")
  key_auth_sort = lambda v: (-v[1], v[0])
  async for doc in curs:
    cocitaithors = tuple(
      dict(
        cocit_author=title, count=cnt,
        frags=dict(
          (fn, sum(map(key_last, cnts)))
          for fn, cnts in groupby(sorted(map(get_fn_cnt, fr)), key=key_first)))
        for title, cnt, fr in sorted(
          map(auth2tuple, doc['auths']), key=key_auth_sort))
    frags = reduce(
      lambda a, b: a+b,  map(Counter, map(get_frags, cocitaithors)))
    if topn_cocitauthors:
      cocitaithors = cocitaithors[:topn_cocitauthors]
    out.append(dict(
      title=doc['title'], type=doc['type'], nka=doc['nka'], count=doc['count'],
      frags=dict(sorted(frags.items())), cocitaithors=cocitaithors))

  return out


@router.get('/ngramms/ngramms/',
  summary='Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»')
async def _req_frags_ngramm_ngramm(
  topn: Optional[int]=10,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  topn_ngramm: Optional[int]=10,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline_root = get_frags_ngramms_ngramms_branch_root(
    topn, authorParams, ngrammParam)
  pipeline_branch = get_frags_ngramms_ngramms_branch_pipeline(ngrammParam)
  if _debug_option == DebugOption.pipeline:
    return dict(pipeline_root=pipeline_root, pipeline_branch=pipeline_branch)

  ngrm2tuple = itemgetter('_id', 'title', 'type', 'nka', 'count', 'conts')
  contexts = slot.mdb.contexts
  topN = tuple([
    ngrm2tuple(doc) async for doc in contexts.aggregate(pipeline_root)])
  exists = frozenset(map(itemgetter(0), topN))

  out_list = []

  for i, (ngrmm, title, typ_, nka, cnt, conts) in enumerate(topN, 1):
    congr = defaultdict(Counter)
    titles = {}
    types = {}

    work_pipeline = [
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}}
    ] + pipeline_branch + [
      {'$match': {'cont.type': typ_}}
    ]
    # _logger.debug('ngrmm: "%s", cnt: %s, pipeline_branch: %s', ngrmm, cnt, work_pipeline)
    # print('ngrmm: "%s", cnt: %s, pipeline_branch: %s' % (ngrmm, cnt, work_pipeline))

    async for doc in contexts.aggregate(work_pipeline):
      cont = doc['cont']
      ngr_id = cont['_id']
      if ngr_id not in exists:
        continue
      fnum = doc['frag_num']
      congr[ngr_id][fnum] += doc['ngrams']['cnt']
      titles[ngr_id] = cont['title']
      types[ngr_id] = cont['type']

    frags = congr.pop(ngrmm)
    crossgrams = []

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crossgrams.append(
        dict(
          title=titles[co], type=types[co], frags=cnts, sum=sum(cnts.values())))
    if topn_ngramm:
      crossgrams = crossgrams[:topn_ngramm]

    out_list.append(
      dict(title=titles[ngrmm], type=typ_, nka=nka, sum=cnt,
        cnt_cross=len(congr), frags=frags, crossgrams=crossgrams))

  return out_list


@router.get('/ngramms/topics/',
  summary='Кросс-распределение «фразы» - «топики контекстов цитирований»')
async def _req_frags_ngramms_topics(
  topn: Optional[int]=10,
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  probability: Optional[float]=.5,
  topn_topics:Optional[int]=10,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_ngramms_topics_pipeline(
    topn, authorParams, ngrammParam, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline, allowDiskUse=True)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]
  out = []
  get_frags = itemgetter('frags')
  get_fn_cnt = itemgetter('fn', 'cnt')
  key_first = itemgetter(0)
  key_last = itemgetter(-1)
  auth2tuple = itemgetter('topic', 'count', "frags")
  key_auth_sort = lambda v: (-v[1], v[0])
  async for doc in curs:
    topics = tuple(
      dict(
        topic=title, count=cnt,
        frags=dict(
          (fn, sum(map(key_last, cnts)))
          for fn, cnts in groupby(sorted(map(get_fn_cnt, fr)), key=key_first)))
        for title, cnt, fr in sorted(
          map(auth2tuple, doc['topics']), key=key_auth_sort))
    if topn_topics:
      topics = topics[:topn_topics]
    frags = reduce(
      lambda a, b: a+b,  map(Counter, map(get_frags, topics)))
    out.append(dict(
      title=doc['title'], type=doc['type'], nka=doc['nka'], count=doc['count'],
      frags=dict(sorted(frags.items())), topics=topics))

  return out


@router.get('/pos_neg/cocitauthors/cocitauthors/',
  summary='Со-цитируемые авторы, распределение тональности их со-цитирований и распределение по 5-ти фрагментам')
async def _req_frags_pos_neg_cocitauthors2(
  topn:Optional[int]=100,
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frag_pos_neg_cocitauthors2(topn, authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline, allowDiskUse=True)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]
  out = []
  async for doc in curs:
    cocitpair = doc['cocitpair']
    conts = doc['conts']
    pubids = tuple(frozenset(map(itemgetter('pubid'), conts)))
    intxtids = tuple(map(itemgetter('cont_id'), conts))
    frags = Counter(map(itemgetter('frag_num'), conts))
    classif = tuple(map(itemgetter('positive_negative'), conts))
    neutral = sum(1 for v in classif  if v['val'] == 0)
    positive = sum(1 for v in classif if v['val'] > 0)
    negative = sum(1 for v in classif if v['val'] < 0)
    out.append(dict(
      cocitpair=tuple(cocitpair.values()),
      cont_cnt=len(intxtids), pub_cnt=len(pubids),
      frags=dict(sorted(frags.items())), neutral=neutral, positive=positive,
      negative=negative, pubids=pubids, intxtids=intxtids))
  return out


@router.get('/pos_neg/contexts/',
  summary='Распределение тональности контекстов по 5-ти фрагментам')
async def _req_frags_pos_neg_contexts(
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frag_pos_neg_contexts(authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]
  return out


@router.get('/publications/',
  summary='Распределение цитирований по 5-ти фрагментам для отдельных публикаций.')
async def _req_frags_pubs(
  authorParams:AuthorParam=Depends(),
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frag_publications(authorParams)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  publications = slot.mdb.publications
  curs = publications.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  to_tuple = itemgetter('_id', 'descr', 'sum', 'frags')
  out = []
  async for doc in curs:
    pubid, descr, sum, frags = to_tuple(doc)

    if len(frags) == 1 and 'fn' not in frags[0]:
      frags = {}
    else:
      frags = dict(map(itemgetter('fn', 'count'), frags))
    out.append(dict(pubid=pubid, descr=descr, sum=sum, frags=frags))

  return out


@router.get('/ref_authors/',) # summary='')
async def _req_frags_refauthors(
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
    frags = Counter(doc.pop('frags', ()))
    doc.update(frags=frags)
    out.append(doc)

  return out


@router.get('/ref_bundles/',) # summary='')
async def _req_frags_refbundles(
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
    frags = Counter(doc.pop('frags', ()))
    out_doc = dict(
      bundle=doc['_id'], year=doc.get('year') or '',
      authors=doc.get('authors') or [], title=doc.get('title') or '',
      total_pubs=doc['total_pubs'], total_cits=doc['total_cits'], frags=frags,
      pubs=doc['pubs'], cits=doc['cits'], pubsids=doc['pubs_ids'])
    out.append(out_doc)

  return out


@router.get('/topics/',
  summary='Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»')
async def _req_frags_topics(
  topn:Optional[int]=None,
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_topics_pipeline(topn, authorParams, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  coll: Collection = slot.mdb.contexts
  curs = coll.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  out = []
  async for doc in curs:
    frags = dict(sorted(map(itemgetter('frag_num', 'count'), doc['frags'])))
    out_dict = dict(name=doc['_id'], count=doc['count'], frags=frags)
    out.append(out_dict)

  return out


@router.get('/topics/cocitauthors/',
  summary='Кросс-распределение «топики» - «со-цитирования»')
async def _req_frags_topics_cocitauthors(
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_topics_cocitauthors_pipeline(authorParams, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  out = []
  get_frag_num = itemgetter('frag_num')
  get_fn = itemgetter('fn')
  topic2tuple = itemgetter('auth', 'count', 'frags')
  key_topic_sort = lambda t: (-t[1], t[0])
  async for doc in curs:
    cocit_author = doc['_id']
    conts = doc['conts']
    frags = Counter(map(get_frag_num, conts))
    crosstopics = tuple(
      dict(
        topic=topic, count=cnt,
        frags=Counter(sorted(map(get_fn, fr))))
        for topic, cnt, fr in sorted(
          map(topic2tuple, doc['auths']), key=key_topic_sort))
    out.append(dict(
      cocit_author=cocit_author, count=doc['count'],
      frags=dict(sorted(frags.items())), crosstopics=crosstopics))

  return out


@router.get('/topics/ngramms/',
  summary='Кросс-распределение «топики» - «фразы»')
async def _req_frags_topics_ngramms(
  authorParams:AuthorParam=Depends(),
  ngrammParam:NgrammParam=Depends(depNgrammParam),
  probability: Optional[float]=.5,
  topn_crpssgramm:Optional[int]=10,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_topics_ngramms_pipeline(
    authorParams, ngrammParam, probability, topn_crpssgramm)
  if _debug_option and _debug_option == DebugOption.pipeline:
    return pipeline

  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline, allowDiskUse=True)
  if _debug_option and _debug_option == DebugOption.raw_out:
    out = [doc async for doc in curs]
    return out
  out = []
  get_frags = itemgetter('frags')
  get_fn_cnt = itemgetter('fn', 'cnt')
  key_first = itemgetter(0)
  key_last = itemgetter(-1)
  async for doc in curs:
    crossgrams = doc['crossgrams']
    for cdoc in crossgrams:
      cfrags = dict(
        (fn, sum(map(key_last, cnts))) for fn, cnts in
        groupby(sorted(map(get_fn_cnt, cdoc['frags'])), key=key_first))
      cdoc['frags'] = cfrags
    frags = reduce(
      lambda a, b: a+b, map(Counter, map(get_frags, crossgrams)))
    out.append(dict(
      topic=doc['topic'], count=doc['count'], frags=dict(sorted(frags.items())),
      crossgrams=crossgrams))
  return out


@router.get('/topics/topics/',
  summary='Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»')
async def _req_frags_topics_topics(
  authorParams:AuthorParam=Depends(),
  probability:Optional[float]=.2,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  pipeline = get_frags_topics_topics_pipeline(authorParams, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]
  out = []
  async for doc in curs:
    crosstopics = doc['crosstopics']
    for cdoc in crosstopics:
      cdoc['frags'] = Counter(sorted(cdoc['frags']))
    frags = reduce(lambda a, b: a + b, map(itemgetter('frags'), crosstopics))
    odoc = dict(
      topic=doc['_id'], count=doc['count'], frags=dict(sorted(frags.items())),
      crosstopics=crosstopics)
    out.append(odoc)
  return out

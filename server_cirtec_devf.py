#! /usr/bin/env python3
# -*- codong: utf-8 -*-
from collections import Counter, defaultdict
from dataclasses import dataclass
import enum
from functools import partial, reduce
from itertools import chain, groupby, islice
import logging
from operator import itemgetter
from typing import Optional

from bson import ObjectId, json_util
from fastapi import APIRouter, FastAPI, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
import uvicorn

from server_dbquery_dev import (
  LType, filter_acc_dict, get_frag_pos_neg_cocitauthors2,
  get_frag_pos_neg_contexts, get_frag_publications,
  get_frags_cocitauthors_cocitauthors_pipeline,
  get_frags_cocitauthors_ngramms_pipeline, get_frags_cocitauthors_pipeline,
  get_frags_cocitauthors_topics_pipeline,
  get_frags_ngramms_cocitauthors_pipeline,
  get_frags_ngramms_ngramms_branch_pipeline,
  get_frags_ngramms_ngramms_branch_root, get_frags_ngramms_pipeline,
  get_frags_ngramms_topics_pipeline, get_frags_topics_cocitauthors_pipeline,
  get_frags_topics_ngramms_pipeline, get_frags_topics_pipeline,
  get_frags_topics_topics_pipeline, get_pos_neg_cocitauthors_pipeline,
  get_pos_neg_contexts_pipeline, get_pos_neg_ngramms_pipeline,
  get_pos_neg_pubs_pipeline, get_pos_neg_topics_pipeline,
  get_publications_cocitauthors_pipeline, get_publications_ngramms_pipeline,
  get_publications_topics_topics_pipeline, get_ref_auth4ngramm_tops_pipeline,
  get_ref_bund4ngramm_tops_pipeline, get_refauthors_part_pipeline,
  get_refauthors_pipeline, get_refbindles_pipeline,
  get_top_cocitauthors_pipeline, get_top_cocitauthors_publications_pipeline,
  get_top_cocitrefs2_pipeline, get_top_cocitrefs_pipeline,
  get_top_detail_bund_refauthors, get_top_ngramms_pipeline,
  get_top_ngramms_publications_pipeline, get_top_topics_pipeline,
  get_top_topics_publications_pipeline)
from server_utils import _init_logging, cvt_oid, to_out_typed
from utils import load_config


_logger = logging.getLogger('cirtec_dev_fastapi')


DEF_AUTHOR = 'Sergey-Sinelnikov-Murylev'


@dataclass(eq=False, order=False)
class Slot:
  conf:dict
  mdb:Database


class DebugOption(enum.Enum):
  pipeline = 'pipeline'
  raw_out = 'raw_out'


slot:Optional[Slot] = None
router = APIRouter()


def main():
  _init_logging()

  # app, conf = create_srv()
  # srv_run_args = conf['srv_run_args']
  # web.run_app(app, **srv_run_args)
  app = FastAPI(
    openapi_url='/cirtec_dev/openapi.json', docs_url='/cirtec_dev/docs',
    redoc_url='/cirtec_dev/redoc',
    description='Сервер данных.'
  )

  conf = _load_conf()
  router.add_event_handler('startup', partial(_init_app, conf))

  app.include_router(router, prefix='/cirtec_dev', )

  # asgi_app = SentryAsgiMiddleware(app)

  conf_app = conf['srv_run_args']
  uvicorn.run(
    app, host=conf_app.get('host') or '0.0.0.0',
    port=conf_app.get('port') or 8668,
    use_colors=True, log_config=None)


def _load_conf() -> dict:
  # env.read_envfile()
  conf = load_config()['dev']

  return conf


async def _init_app(conf:dict):
  global slot
  mconf = conf['mongodb']
  mcli = AsyncIOMotorClient(mconf['uri'], compressors='zstd,snappy,zlib')
  mdb = mcli[mconf['db']] #.mail_links
  slot = Slot(conf, mdb)


@router.on_event('shutdown')
async def _close_app():
  global slot
  slot.mdb.client.close()


@router.get('/db/bundle/',
  summary='Данные по указанному бандлу (bundles) из mongodb')
async def _db_bundle(id:str):
  _logger.info('start func(%s)', id)
  coll:Collection = slot.mdb.bundles
  doc:dict = await coll.find_one(dict(_id=id))
  _logger.info('end func(%s)->%s', id, doc)
  return doc


@router.get('/db/context/',
  summary='Данные по указанному контексту (contexts) из mongodb')
async def _db_context(id: str):
  coll:Collection = slot.mdb.contexts
  doc:dict = await coll.find_one(dict(_id=id))
  if doc and 'topics' in doc:
    doc['topics'] = [cvt_oid(**t) for t in doc['topics']]
  return doc


@router.get('/db/ngramm/',
  summary='Данные по указанной нграмме (n_gramms) из mongodb')
async def _db_topic(id: str):
  coll:Collection = slot.mdb.n_gramms
  doc:dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/db/publication/',
  summary='Данные по указанному публикации (publications) из mongodb')
async def _db_publication(id: str):
  coll:Collection = slot.mdb.publications
  doc:dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/db/topic/',
  summary='Данные по указанному топику (topics) из mongodb')
async def _db_topic(id: str):
  coll: Collection = slot.mdb.topics
  doc: dict = await coll.find_one(dict(_id=ObjectId(id)))
  if doc:
    return cvt_oid(**doc)
  return doc


@router.get('/publications/',
  summary='Публикации')
async def _req_publications(
  author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None
):
  query = {
    'name': {'$exists': 1},
    **filter_acc_dict(author, cited, citing)}

  to_out = partial(to_out_typed, type='publication')

  publications = slot.mdb.publications
  out = [
    to_out(**doc) async for doc in
      publications.find(query).sort([('year', ASCENDING), ('_id', ASCENDING)])]

  return out


@router.get('/frags/cocitauthors/',
  summary='Распределение «со-цитируемые авторы» по 5-ти фрагментам')
async def _req_frags_cocitauthors(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, _add_pipeline:bool=False
):
  coll: Collection = slot.mdb.contexts
  pipeline = get_frags_cocitauthors_pipeline(topn, author, cited, citing)
  out = []
  async for doc in coll.aggregate(pipeline):
    frags = Counter(doc['frags'])
    out_dict = dict(name=doc['_id'], count=doc['count'], frags=frags)
    out.append(out_dict)

  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/frags/cocitauthors/cocitauthors/',
  summary='Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»')
async def _req_frags_cocitauthors_cocitauthors(
  topn:Optional[int]=100, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None,
  _debug_option:DebugOption=None
):
  pipeline = get_frags_cocitauthors_cocitauthors_pipeline(
    topn, author, cited, citing)
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


@router.get('/frags/cocitauthors/ngramms/',
  summary='Кросс-распределение «со-цитирования» - «фразы из контекстов цитирований»')
async def _req_frags_cocitauthors_ngramms(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None,
  nka:Optional[int]=Query(None, ge=0, le=6),
  ltype:Optional[LType]=Query(None, title='Тип фразы'),
  topn_gramm:Optional[int]=10,
  _debug_option:DebugOption=None
):
  pipeline = get_frags_cocitauthors_ngramms_pipeline(
    topn, author, cited, citing, nka, ltype)
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


@router.get('/frags/cocitauthors/topics/',
  summary='Кросс-распределение «со-цитирования» - «топики контекстов цитирований»')
async def _req_frags_cocitauthors_topics(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, probability:Optional[float]=.5,
  _add_pipeline:bool=False
):
  pipeline = get_frags_cocitauthors_topics_pipeline(
    topn, author, cited, citing, probability)
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  # out = [doc async for doc in curs]
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
  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/frags/ngramms/',
  summary='Распределение «5 фрагментов» - «фразы из контекстов цитирований»')
async def _req_frags_cocitauthors(
  topn:Optional[int]=10, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None,
  nka:Optional[int]=Query(None, ge=0, le=6),
  ltype:Optional[LType]=Query(None, title='Тип фразы'), #, description='Может быть одно из значений "lemmas", "nolemmas" или пустой'),
  _debug_oprion:DebugOption=None
):
  pipeline = get_frags_ngramms_pipeline(topn, author, cited, citing, nka, ltype)
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


@router.get('/frags/ngramms/cocitauthors/',
  summary='Кросс-распределение «фразы» - «со-цитирования»')
async def _req_frags_ngramms_cocitauthors(
  topn: Optional[int] = 10, author: Optional[str] = None,
  cited: Optional[str] = None, citing: Optional[str] = None,
  nka: Optional[int] = Query(None, ge=0, le=6),
  ltype: Optional[LType] = Query(None, title='Тип фразы'),
  topn_cocitauthors: Optional[int] = None,
  _debug_option:DebugOption=None
):
  pipeline = get_frags_ngramms_cocitauthors_pipeline(
    topn, author, cited, citing, nka, ltype)
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


@router.get('/frags/ngramms/ngramms/',
  summary='Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»')
async def _req_frags_ngramm_ngramm(
  topn: Optional[int]=10,
  author: Optional[str] = None, cited: Optional[str] = None,
  citing: Optional[str] = None, nka: Optional[int] = Query(None, ge=0, le=6),
  ltype: Optional[LType] = Query(None, title='Тип фразы'),
  topn_ngramm: Optional[int]=10,
  _debug_option:DebugOption=None
):
  pipeline_root = get_frags_ngramms_ngramms_branch_root(
    topn, author, cited, citing, nka, ltype)
  pipeline_branch = get_frags_ngramms_ngramms_branch_pipeline(nka, ltype)
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


@router.get('/frags/ngramms/topics/',
  summary='Кросс-распределение «фразы» - «топики контекстов цитирований»')
async def _req_frags_ngramms_topics(
  topn: Optional[int]=10,
  author: Optional[str]=None, cited: Optional[str]=None,
  citing: Optional[str]=None, nka: Optional[int]=Query(None, ge=0, le=6),
  ltype: Optional[LType]=Query(None, title='Тип фразы'),
  probability: Optional[float]=.5,
  topn_topics:Optional[int]=10,
  _debug_option:DebugOption=None
):
  pipeline = get_frags_ngramms_topics_pipeline(
    topn, author, cited, citing, nka, ltype, probability)
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


@router.get('/frags/pos_neg/cocitauthors/cocitauthors/',
  summary='Со-цитируемые авторы, распределение тональности их со-цитирований и распределение по 5-ти фрагментам')
async def _req_frags_pos_neg_cocitauthors2(
  topn:Optional[int]=100, author: Optional[str] = None,
  cited:Optional[str]=None, citing:Optional[str]=None,
  _debug_option:DebugOption=None
):
  pipeline = get_frag_pos_neg_cocitauthors2(topn, author, cited, citing)
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


@router.get('/frags/pos_neg/contexts/',
  summary='Распределение тональности контекстов по 5-ти фрагментам')
async def _req_frags_pos_neg_contexts(
  author: Optional[str] = None, cited: Optional[str] = None,
  citing: Optional[str] = None,
  _debug_option:DebugOption=None
):
  pipeline = get_frag_pos_neg_contexts(author, cited, citing)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]
  return out


@router.get('/frags/publications/',
  summary='Распределение цитирований по 5-ти фрагментам для отдельных публикаций.')
async def _req_frags_pubs(
  author: Optional[str] = None, cited: Optional[str] = None,
  citing: Optional[str] = None,
  _debug_option:DebugOption=None
):
  pipeline = get_frag_publications(author, cited, citing)
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


@router.get('/frags/ref_authors/',) # summary='')
async def _req_frags_refauthors(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None,
  _debug_option:DebugOption=None
):
  pipeline = get_refauthors_pipeline(topn, author, cited, citing)
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


@router.get('/frags/ref_bundles/',) # summary='')
async def _req_frags_refbundles(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None,
  _debug_option:DebugOption=None
):
  pipeline = get_refbindles_pipeline(topn, author, cited, citing)
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


@router.get('/frags/topics/',
  summary='Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»')
async def _req_frags_topics(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, probability:Optional[float]=.5,
  _debug_option:DebugOption=None
):
  pipeline = get_frags_topics_pipeline(topn, author, cited, citing, probability)
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


@router.get('/frags/topics/cocitauthors/',
  summary='Кросс-распределение «топики» - «со-цитирования»')
async def _req_frags_topics_cocitauthors(
  author: Optional[str] = None, cited: Optional[str] = None,
  citing: Optional[str] = None, probability:Optional[float]=.5,
  _add_pipeline: bool = False
):
  pipeline = get_frags_topics_cocitauthors_pipeline(
    author, cited, citing, probability)
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  # out = [doc async for doc in curs]
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
  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/frags/topics/ngramms/',
  summary='Кросс-распределение «топики» - «фразы»')
async def _req_frags_topics_ngramms(
  author: Optional[str] = None, cited: Optional[str] = None,
  citing: Optional[str] = None,
  nka: Optional[int] = Query(None, ge=0, le=6),
  ltype: Optional[LType] = Query(None, title='Тип фразы'),
  probability: Optional[float] = .5,
  topn_crpssgramm:Optional[int]=10,
  _debug_option: DebugOption=None
):
  pipeline = get_frags_topics_ngramms_pipeline(
    author, cited, citing, nka, ltype, probability, topn_crpssgramm)
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


@router.get('/frags/topics/topics/',
  summary='Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»')
async def _req_frags_topics_topics(
  author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, probability:Optional[float]=.2,
  _debug_option:DebugOption=None
):
  pipeline = get_frags_topics_topics_pipeline(
    author, cited, citing, probability)
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


@router.get('/pos_neg/cocitauthors/',
  summary='для каждого класса тональности привести топ со-цитируемых авторов')
async def _req_pos_neg_cocitauthors(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, _debug_option:DebugOption=None
):
  pipeline = get_pos_neg_cocitauthors_pipeline(topn, author, cited, citing)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]
  return out


@router.get('/pos_neg/contexts/',
  summary='для каждого класса тональности показать общее количество контекстов')
async def _req_pos_neg_contexts(
  author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, _debug_option: DebugOption = None
):
  pipeline = get_pos_neg_contexts_pipeline(author, cited, citing)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]
  return out


@router.get('/pos_neg/ngramms/',
  summary='для каждого класса тональности показать топ фраз с количеством повторов каждой')
async def _req_pos_neg_ngramms(
  topn:Optional[int]=10, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, nka:Optional[int]=Query(None, ge=0, le=6),
  ltype:Optional[LType]=Query(None, title='Тип фразы'), #, description='Может быть одно из значений "lemmas", "nolemmas" или пустой'),
  _debug_option:DebugOption=None
):
  pipeline = get_pos_neg_ngramms_pipeline(
    topn, author, cited, citing, nka, ltype)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]
  return out


@router.get('/pos_neg/pubs/',) # summary='Топ N со-цитируемых референсов')
async def _req_pos_neg_pubs(
  author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, _debug_option:DebugOption=None
):
  pipeline = get_pos_neg_pubs_pipeline(author, cited, citing)
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


@router.get('/pos_neg/ref_authors/',) # summary='Топ N со-цитируемых референсов')
async def _req_pos_neg_refauthors(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, _add_pipeline: bool = False
):
  pipeline = get_refauthors_pipeline(topn, author, cited, citing)

  contexts:Collection = slot.mdb.contexts
  out = []
  async for row in contexts.aggregate(pipeline):
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
  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/pos_neg/ref_bundles/',) # summary='Топ N со-цитируемых референсов')
async def _req_pos_neg_refbundles(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, _add_pipeline: bool = False
):
  pipeline = get_refbindles_pipeline(topn, author, cited, citing)

  contexts:Collection = slot.mdb.contexts
  out = []
  async for doc in contexts.aggregate(pipeline):
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
  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/pos_neg/topics/',
  summary='для каждого класса тональности показать топ топиков с количеством')
async def _req_pos_neg_topics(
  author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, probability:Optional[float]=.5,
  _add_pipeline: bool = False
):
  pipeline = get_pos_neg_topics_pipeline(author, cited, citing, probability)
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]
  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/publ/ngramms/ngramms/',
  summary='Кросс-распределение «публикации» - «фразы из контекстов цитирований»')
async def _req_publ_ngramm_ngramm(
  topn:Optional[int]=10, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, nka:Optional[int]=Query(None, ge=0, le=6),
  ltype:Optional[LType]=Query(None, title='Тип фразы'),
  topn_ngramm:Optional[int]=10,
  _debug_option: DebugOption = None
):
  pipeline_root = get_frags_ngramms_ngramms_branch_root(topn, author, cited,
    citing, nka, ltype)
  pipeline_branch = get_frags_ngramms_ngramms_branch_pipeline(nka, ltype)
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


@router.get('/publ/publications/cocitauthors/',
  summary='Кросс-распределение «со-цитируемые авторы» по публикациям')
async def _req_publ_publications_cocitauthors(
  author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, topn_auth:Optional[int]=None,
  _debug_option: DebugOption=None
):
  pipeline = get_publications_cocitauthors_pipeline(
    author, cited, citing, topn_auth)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    out = [doc async for doc in curs]
    return out

  out = [doc async for doc in curs]
  return out


@router.get('/publ/publications/ngramms/',
  summary='Кросс-распределение «фразы из контекстов цитирований» по публикациям')
async def _req_publ_publications_ngramms(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, nka:Optional[int]=Query(None, ge=0, le=6),
  ltype:Optional[LType]=Query(None, title='Тип фразы'),
  topn_gramm:Optional[int] = 10,
  _debug_option: DebugOption = None
):
  pipeline = get_publications_ngramms_pipeline(
    topn, author, cited, citing, nka, ltype, topn_gramm)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)

  out = [doc async for doc in curs]
  return out


@router.get('/publ/topics/topics/',
  summary='Кросс-распределение «публикации» - «топики контекстов цитирований»')
async def _req_publ_topics_topics(
  author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, probability:Optional[float]=.4,
  _debug_option: DebugOption = None
):
  pipeline = get_publications_topics_topics_pipeline(
    author, cited, citing, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  contexts = slot.mdb.contexts
  curs = contexts.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    out = [doc async for doc in curs]
    return out

  out = [doc async for doc in curs]
  return out


@router.get('/pubs/ref_authors/',) # summary='Топ N со-цитируемых референсов')
async def _req_pubs_refauthors(
  top_auth:Optional[int]=3, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None,
  _debug_option:DebugOption=None
):
  pipeline = get_refauthors_part_pipeline(top_auth, None, None, None)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  publications: Collection = slot.mdb.publications
  contexts: Collection = slot.mdb.contexts


  out = []
  async for pub in publications.find(
    # {'uni_authors': 'Sergey-Sinelnikov-Murylev'},
    {'name': {'$exists': 1}, **filter_acc_dict(author, cited, citing),},
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


@router.get('/top/cocitauthors/',
  summary='Топ N со-цитируемых авторов')
async def _req_top_cocitauthors(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, _add_pipeline:bool=False
):
  pass
  coll: Collection = slot.mdb.contexts
  pipeline = get_top_cocitauthors_pipeline(topn, author, cited, citing)

  out = []
  async for doc in coll.aggregate(pipeline):
    title = doc.pop('_id')
    out.append(dict(title=title, **doc))
  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/top/cocitrefs/',
  summary='Топ N со-цитируемых референсов')
async def _req_top_cocitauthors(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None,
  _debug_option:DebugOption=None
):
  pipeline = get_top_cocitrefs_pipeline(topn, author, cited, citing)
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


@router.get('/top/cocitauthors/publications/',
  summary='Топ N со-цитируемых авторов по публикациям')
async def _req_top_cocitauthors_pubs(
  topn:Optional[int]=None, author: Optional[str]=None,
  cited: Optional[str]=None, citing: Optional[str]=None,
  _debug_option:DebugOption=None
):
  pipeline = get_top_cocitauthors_publications_pipeline(
    topn, author, cited, citing)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out


@router.get('/top/cocitrefs/cocitrefs/',
  summary='Топ N со-цитируемых авторов по публикациям')
async def _req_top_cocitrefs2(
  topn: Optional[int] = None, author: Optional[str] = None,
  cited: Optional[str] = None, citing: Optional[str] = None,
  _debug_option: DebugOption = None
):
  pipeline = get_top_cocitrefs2_pipeline(topn, author, cited, citing)
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


@router.get('/top/ngramms/',
  summary='Топ N фраз по публикациям')
async def _req_top_ngramms(
  topn:Optional[int]=10, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None,
  nka:Optional[int]=Query(None, ge=0, le=6),
  ltype:Optional[LType]=Query(None, title='Тип фразы'), #, description='Может быть одно из значений "lemmas", "nolemmas" или пустой'),
  _debug_option:Optional[DebugOption]=None
):
  pipeline = get_top_ngramms_pipeline(topn, author, cited, citing, nka, ltype)
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


@router.get('/top/ngramms/publications/',
  summary='Топ N фраз по публикациям')
async def _req_top_ngramm_pubs(
  topn:Optional[int]=10, author: Optional[str]=None,
  cited: Optional[str]=None, citing: Optional[str]=None,
  nka:Optional[int]=Query(None, ge=0, le=6),
  ltype:Optional[LType]=Query(None, title='Тип фразы'),
  _debug_option:DebugOption=None
):
  pipeline = get_top_ngramms_publications_pipeline(
    topn, author, cited, citing, nka, ltype)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out


@router.get('/top/ref_authors/',
  summary='Топ N авторов бандлов')
async def _req_top_ref_bundles(
  topn: Optional[int] = None,  author: Optional[str] = None,
  cited: Optional[str] = None, citing: Optional[str] = None,
  _debug_option:Optional[DebugOption]=None
):
  pipeline = get_refauthors_pipeline(topn, author, cited, citing)
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
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, _debug_option:Optional[DebugOption]=None
):
  pipeline = get_refbindles_pipeline(topn, author, cited, citing)
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
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, probability:Optional[float]=.5,
  _debug_option:DebugOption=None
):
  pipeline = get_top_topics_pipeline(topn, author, cited, citing, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  coll: Collection = slot.mdb.contexts
  out = [doc async for doc in coll.aggregate(pipeline)]
  return out


@router.get('/top/topics/publications/',
  summary='Топ N топиков')
async def _req_top_topics_pubs(
  topn:Optional[int]=None, author: Optional[str]=None,
  cited: Optional[str]=None, citing: Optional[str]=None,
  probability:Optional[float]=.5,
  _debug_option: DebugOption = None
):
  pipeline = get_top_topics_publications_pipeline(
    topn, author, cited, citing, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts: Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out


@router.get('/ref_auth4ngramm_tops/',) # summary='Топ N со-цитируемых референсов')
async def _ref_auth4ngramm_tops(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, probability:Optional[float]=.5,
  _debug_option: DebugOption = None
):
  pipeline = get_ref_auth4ngramm_tops_pipeline(topn, author, cited, citing)
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
  topn:Optional[int]=None, author: Optional[str]=None,
  cited: Optional[str]=None, citing: Optional[str]=None,
  probability: Optional[float]=.5,
  _add_pipeline: bool = False
):
  contexts: Collection = slot.mdb.contexts

  out_bund = []
  pipeline = get_ref_bund4ngramm_tops_pipeline(topn, author, cited, citing)

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
  async for cont in contexts.aggregate(pipeline):
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
    out_bund.append(cont)
  out = out_bund

  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/by_frags/ref_authors/',) # summary='Топ N со-цитируемых референсов')
async def _req_by_frags_refauthors(
  topn:Optional[int]=None, author: Optional[str]=None,
  cited: Optional[str]=None, citing: Optional[str]=None,
  _add_pipeline: bool = False
):
  contexts: Collection = slot.mdb.contexts
  pipeline = get_refauthors_part_pipeline(topn, author, cited, citing)
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

  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/top_detail_bund/ref_authors/',) # summary='Топ N со-цитируемых референсов')
async def _req_top_detail_bund_refauthors(
  topn:Optional[int]=None, author: Optional[str]=None,
  cited: Optional[str]=None, citing: Optional[str]=None,
  _debug_option:DebugOption=None
):
  pipeline = get_top_detail_bund_refauthors(topn, author, cited, citing)
  if _debug_option == DebugOption.pipeline:
    return pipeline

  contexts:Collection = slot.mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]
  return out


if __name__ == '__main__':
  main()

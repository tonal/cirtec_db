#! /usr/bin/env python3
# -*- codong: utf-8 -*-
import asyncio
from collections import Counter
from functools import partial
from itertools import chain, groupby, islice
import logging
from operator import itemgetter
from statistics import mean, pstdev
from typing import Union

from aiohttp import web
from fastnumbers import fast_float
import motor.motor_asyncio as aiomotor
from pymongo import ASCENDING
from pymongo.collection import Collection
import uvloop

from server_cirtec import (
  _req_frags, _req_frags_pubs, _req_publ_publications_cocitauthors,
  _req_publ_publications_topics, _req_frags_cocitauthors,
  _req_frags_cocitauthors_cocitauthors, _req_publ_cocitauthors_cocitauthors,
  _req_frags_ngramm, _req_frags_ngramm_ngramm, _req_publ_ngramm_ngramm,
  _req_frags_topics, _req_frags_topics_topics, _req_publ_topics_topics,
  _req_frags_cocitauthors_ngramms, _req_frags_cocitauthors_topics,
  _req_frags_ngramms_cocitauthors, _req_publ_publications_ngramms,
  _req_frags_ngramms_topics, _req_frags_topics_cocitauthors,
  _req_frags_topics_ngramms, _req_top_cocitauthors, _req_top_cocitauthors_pubs,
  _reg_cnt_ngramm, _reg_cnt_pubs_ngramm)
from server_utils import (
  getreqarg_topn, json_response, _init_logging, getreqarg_int, getreqarg,
  getreqarg_probability)
from utils import load_config


_logger = logging.getLogger('cirtec_dev')


def main():
  uvloop.install()

  _init_logging()

  app, conf = create_srv()

  srv_run_args = conf['srv_run_args']
  web.run_app(app, **srv_run_args)


def create_srv():
  conf = _load_conf()

  app = web.Application(middlewares=[error_pages])
  router = app.router
  add_get = partial(router.add_get, allow_head=False)

  _add_old_reqs(add_get)

  add_get('/cirtec_dev/top/ref_bundles/', _req_top_refbundles)
  add_get('/cirtec_dev/top/ref_authors/', _req_top_refauthors)
  add_get('/cirtec_dev/pubs/ref_authors/', _req_pubs_refauthors)
  add_get('/cirtec_dev/ref_auth_bund4ngramm_tops/', _req_auth_bund4ngramm_tops)
  add_get('/cirtec_dev/ref_bund4ngramm_tops/', _req_bund4ngramm_tops)
  add_get('/cirtec_dev/ref_auth4ngramm_tops/', _ref_auth4ngramm_tops)
  add_get('/cirtec_dev/pos_neg/pubs/', _req_pos_neg_pubs)
  add_get('/cirtec_dev/pos_neg/ref_bundles/', _req_pos_neg_refbundles)
  add_get('/cirtec_dev/pos_neg/ref_authors/', _req_pos_neg_refauthors)
  add_get('/cirtec_dev/frags/ref_bundles/', _req_frags_refbundles)
  add_get('/cirtec_dev/frags/ref_authors/', _req_frags_neg_refauthors)
  add_get('/cirtec_dev/publications/', _req_publications)


  app['conf'] = conf
  app['tasks'] = set()
  app.cleanup_ctx.append(_db_context)
  app.on_cleanup.append(_clean_tasks)

  return app, conf


def _add_old_reqs(add_get):
  # А Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций
  add_get(r'/cirtec_dev/frags/', _req_frags)
  #   Распределение цитирований по 5-ти фрагментам для отдельных публикаций. #заданного автора.
  add_get(r'/cirtec_dev/frags/publications/', _req_frags_pubs)
  #   Кросс-распределение «со-цитируемые авторы» по публикациям
  add_get(r'/cirtec_dev/publ/publications/cocitauthors/',
    _req_publ_publications_cocitauthors)
  #   Кросс-распределение «фразы из контекстов цитирований» по публикациям
  add_get(r'/cirtec_dev/publ/publications/ngramms/', _req_publ_publications_ngramms)
  #   Кросс-распределение «топики контекстов цитирований» по публикациям
  add_get(r'/cirtec_dev/publ/publications/topics/', _req_publ_publications_topics)
  #   Распределение «со-цитируемые авторы» по 5-ти фрагментам
  add_get(r'/cirtec_dev/frags/cocitauthors/', _req_frags_cocitauthors)
  #   Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»
  add_get(r'/cirtec_dev/frags/cocitauthors/cocitauthors/',
    _req_frags_cocitauthors_cocitauthors)
  #   Кросс-распределение «публикации» - «со-цитируемые авторы»
  add_get(r'/cirtec_dev/publ/cocitauthors/cocitauthors/',
    _req_publ_cocitauthors_cocitauthors)
  #   Распределение «5 фрагментов» - «фразы из контекстов цитирований»
  add_get(r'/cirtec_dev/frags/ngramms/', _req_frags_ngramm)
  #   Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»
  add_get(r'/cirtec_dev/frags/ngramms/ngramms/', _req_frags_ngramm_ngramm)
  #   Кросс-распределение «публикации» - «фразы из контекстов цитирований»
  add_get(r'/cirtec_dev/publ/ngramms/ngramms/', _req_publ_ngramm_ngramm)
  #   Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»
  add_get(r'/cirtec_dev/frags/topics/', _req_frags_topics)
  #   Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»
  add_get(r'/cirtec_dev/frags/topics/topics/', _req_frags_topics_topics)
  #   Кросс-распределение «публикации» - «топики контекстов цитирований»
  add_get(r'/cirtec_dev/publ/topics/topics/', _req_publ_topics_topics)
  # Б Кросс-распределение «со-цитирования» - «фразы из контекстов цитирований»
  add_get(r'/cirtec_dev/frags/cocitauthors/ngramms/',
    _req_frags_cocitauthors_ngramms)
  #   Кросс-распределение «со-цитирования» - «топики контекстов цитирований»
  add_get(r'/cirtec_dev/frags/cocitauthors/topics/', _req_frags_cocitauthors_topics)
  # В Кросс-распределение «фразы» - «со-цитирования»
  add_get(r'/cirtec_dev/frags/ngramms/cocitauthors/',
    _req_frags_ngramms_cocitauthors)
  #   Кросс-распределение «фразы» - «топики контекстов цитирований»
  add_get(r'/cirtec_dev/frags/ngramms/topics/', _req_frags_ngramms_topics)
  # Г Кросс-распределение «топики» - «со-цитирования»
  add_get(r'/cirtec_dev/frags/topics/cocitauthors/', _req_frags_topics_cocitauthors)
  #   Кросс-распределение «топики» - «фразы»
  add_get(r'/cirtec_dev/frags/topics/ngramms/', _req_frags_topics_ngramms)
  # Топ N со-цитируемых авторов
  add_get(r'/cirtec_dev/top/cocitauthors/', _req_top_cocitauthors)
  # Топ N со-цитируемых авторов по публикациям
  add_get(r'/cirtec_dev/top/cocitauthors/publications/', _req_top_cocitauthors_pubs)
  # Топ N фраз
  add_get(r'/cirtec_dev/top/ngramms/', _req_top_ngramm)
  # Топ N фраз по публикациям
  add_get(r'/cirtec_dev/top/ngramms/publications/', _req_top_ngramm_pubs)
  # Топ N топиков
  add_get(r'/cirtec_dev/top/topics/', _req_top_topics)
  # Топ N топиков публикациям
  add_get(r'/cirtec_dev/top/topics/publications/', _req_top_topics_pubs)
  add_get(r'/cirtec_dev/cnt/ngramms/', _reg_cnt_ngramm)
  add_get(r'/cirtec_dev/cnt/publications/ngramms/', _reg_cnt_pubs_ngramm)


def _load_conf() -> dict:
  # env.read_envfile()
  conf = load_config()['dev']

  return conf


@web.middleware
async def error_pages(request:web.Request, handler:callable):
  try:
    response = await handler(request)
    if response and response.status >= 500:
      await handle_500(request, response)
    return response
  except web.HTTPException as ex:
    if ex.status >= 500:
      await handle_500(request, ex)
    raise
  # except Can
  except Exception as ex:
    await handle_500(request, ex)
    raise


async def handle_500(
  request:web.Request, response:Union[web.Response, Exception]
):
  exc_info = isinstance(response, Exception)
  text = (await request.text()).strip()
  headers = '\n\t'.join(f'{k}: {v}' for k, v in request.headers.items())
  if text:
    text = '\n\t'.join(text.splitlines())
    text = f'\nДанные:\n\t{text}'
  _logger.error(
    f'Ошибка при обработке запроса: {request.method} {request.rel_url}\n'
    f'Заголовки: \n\t{headers}{text}',
    exc_info=exc_info)


async def _db_context(app):
  conf = app['conf']['mongodb']

  conn = aiomotor.AsyncIOMotorClient(conf['uri'], compressors='snappy') # type_registry=decimal_type_registry
  db = conn[conf['db']]
  app['db'] = db

  yield

  conn.close()


async def _clean_tasks(app):
  tasks = app['tasks']
  canceled = set()
  for t in tasks:
    # type: t:asyncio.Task
    if not t.done():
      t.cancel()
      canceled.add(t)
  if canceled:
    await asyncio.wait(canceled, return_when='ALL_COMPLETED')


async def _req_top_refbundles(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refbindles_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  async for doc in contexts.aggregate(pipeline):
    doc.pop('pos_neg', None)
    doc.pop('frags', None)
    out.append(doc)

  return json_response(out)


def _get_refbindles_pipeline(topn:int=None):
  pipeline = [
    {'$match': {'exact': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False, 'linked_papers_ngrams': False}},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},  ##
    {'$group': {
      '_id': '$bundles', 'cits': {'$sum': 1}, 'pubs': {'$addToSet': '$pub_id'},
      'pos_neg': {'$push': '$positive_negative'},
      'frags': {'$push': '$frag_num'}, }},
    {'$unwind': '$pubs'},
    {'$group': {
      '_id': '$_id', 'cits': {'$first': '$cits'}, 'pubs': {'$sum': 1},
      'pos_neg': {'$first': '$pos_neg'}, 'frags': {'$first': '$frags'}, }},
    # 'pubs_ids': {'$addToSet': '$pubs'}, }},
    {'$lookup': {
      'from': 'bundles', 'localField': '_id', 'foreignField': '_id',
      'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$project': {
      'cits': True, 'pubs': True, 'pubs_ids': True,
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title', 'pos_neg': True, 'frags': True, }},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}}, # {$count: 'cnt'}
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


async def _req_top_refauthors(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refauthors_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  async for row in contexts.aggregate(pipeline):
    row.pop('pos_neg', None)
    row.pop('frags', None)
    out.append(row)

  return json_response(out)


def _get_refauthors_pipeline(topn:int=None):
  pipeline = [
    {'$match': {'exact': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False, 'linked_papers_ngrams': False}},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
      'as': 'bun'}},
    {'$unwind': '$bun'},
    {'$unwind': '$bun.authors'},
    {'$group': {
      '_id': '$bun.authors', 'cits': {'$addToSet': '$_id'},
      'pubs': {'$addToSet': '$pub_id'},
      'binds': {
          '$addToSet': {
            '_id': '$bun._id', 'total_cits': '$bun.total_cits',
            'total_pubs': '$bun.total_pubs'}},
      'pos_neg': {'$push': '$positive_negative'},
      'frags': {'$push': '$frag_num'}}},
    {'$project': {
      '_id': False, 'author': '$_id', 'cits': {'$size': '$cits'},
      'pubs': {'$size': '$pubs'}, 'total_cits': {'$sum': '$binds.total_cits'},
      'total_pubs': {'$sum': '$binds.total_pubs'},
      'pos_neg': True, 'frags': True}},
    {'$sort': {'cits': -1, 'pubs': -1, 'author': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


async def _req_pubs_refauthors(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  publications:Collection = mdb.publications
  contexts:Collection = mdb.contexts

  pipeline = _get_refauthors_pipeline(3)

  out = []
  async for pub in publications.find(
    {'uni_authors': 'Sergey-Sinelnikov-Murylev'},
    projection={'_id': True, 'name': True}, sort=[('_id', ASCENDING)]
  ):
    pid = pub['_id']
    pub_pipeline = [{'$match': {'pub_id': pid}}] + pipeline
    ref_authors = []
    async for row in contexts.aggregate(pub_pipeline):
      row.pop('pos_neg', None)
      row.pop('frags', None)
      ref_authors.append(row)

    pub_out = dict(pub_id=pid, name=pub['name'], ref_authors=ref_authors)
    out.append(pub_out)
  return json_response(out)


async def _req_auth_bund4ngramm_tops(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  contexts:Collection = mdb.contexts
  out = dict()

  a_pipeline = _get_refauthors_pipeline()
  a_pipeline += [{'$match': {'cits': 5}}]

  out_acont = []
  ref_authors = frozenset(
    [o['author'] async for o in contexts.aggregate(a_pipeline)])
  async for cont in contexts.aggregate([
    {'$match': {'exact': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False, 'linked_papers_ngrams': False}},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
      'as': 'bun'}},
    {'$unwind': '$bun'},
    {'$unwind': '$bun.authors'},
    {'$match': {'bun.authors': {'$in': list(ref_authors)}}},
    {'$group': {
      '_id': '$_id', 'cnt': {'$sum': 1},
      'authors': {'$addToSet': '$bun.authors'}}},
    {'$sort': {'cnt': -1, '_id': 1}},
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}},
    {'$project': {
      'cont.prefix': False, 'cont.suffix': False, 'cont.exact': False, }},
    {'$unwind': '$cont'},
  ]):
    icont = cont['cont']
    ngramms = icont.get('linked_papers_ngrams')
    oauth = dict(
      cont_id=cont['_id'], ref_authors=cont['authors'],
      topics=icont['linked_papers_topics'])
    if ngramms:
      ongs = sorted(
        (
          dict(ngramm=n, type=t, cnt=c)
          for (t, n), c in (
            (ngr['_id'].split('_'), ngr['cnt']) for ngr in ngramms)
          if t == 'lemmas' and len(n.split()) == 2
        ),
        key=lambda o: (-o['cnt'], o['type'], o['ngramm'])
      )
      oauth.update(ngramms=ongs[:5])
    out_acont.append(oauth)

  out.update(ref_auth_conts=out_acont)

  b_pipeline = _get_refbindles_pipeline()
  b_pipeline += [{'$match': {'cits': 5}}]
  out_bund = []
  ref_bund = frozenset([
    o['_id'] async for o in contexts.aggregate(b_pipeline)])
  async for cont in contexts.aggregate([
    {'$match': {'exact': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False, 'linked_papers_ngrams': False}},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$match': {'bundles': {'$in': list(ref_bund)}}},
    {'$group': {'_id': '$_id', 'cnt': {'$sum': 1}}},
    {'$sort': {'cnt': -1, '_id': 1}},
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}},
    {'$project': {
      'cont.prefix': False, 'cont.suffix': False, 'cont.exact': False, }},
    {'$unwind': '$cont'},
    {'$lookup': {
      'from': 'bundles', 'localField': 'cont.bundles', 'foreignField': '_id',
      'as': 'bund'}},
  ]):
    icont = cont['cont']
    ngramms = icont.get('linked_papers_ngrams')
    bundles = [
      dict(
        bundle=b['_id'], year=b['year'], title=b['title'],
        authors=b.get('authors'))
      for b in cont['bund']]
    oauth = dict(
      cont_id=cont['_id'], bundles=bundles,
      topics=icont['linked_papers_topics'])
    if ngramms:
      ongs = sorted(
        (
          dict(ngramm=n, type=t, cnt=c)
          for (t, n), c in (
            (ngr['_id'].split('_'), ngr['cnt']) for ngr in ngramms)
          if t == 'lemmas' and len(n.split()) == 2
        ),
        key=lambda o: (-o['cnt'], o['type'], o['ngramm'])
      )
      oauth.update(ngramms=ongs[:5])
    out_bund.append(oauth)

  out.update(ref_bundles=out_bund)

  return json_response(out)


async def _req_bund4ngramm_tops(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)
  probability = getreqarg_probability(request)

  contexts:Collection = mdb.contexts

  out_bund = []
  pipeline = [
    {'$match': {'exact': {'$exists': True}}}, {
    '$project': {'prefix': False, 'suffix': False, 'exact': False, }},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$group': {
      '_id': '$bundles', 'cits': {'$sum': 1}, 'pubs': {'$addToSet': '$pub_id'},
      'conts': {'$addToSet': {
        'cid': '$_id', 'topics': '$linked_papers_topics',
        'ngrams': '$linked_papers_ngrams'}}}},
    {'$lookup': {
      'from': 'bundles', 'localField': '_id', 'foreignField': '_id',
      'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$project': {
      '_id': False,
      'bundle': '$_id', 'cits': True, 'pubs': {'$size': '$pubs'},
      'pubs_ids': '$pubs', 'conts': True,
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title',}},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  get_probab = itemgetter('probability')
  get_first = itemgetter(0)
  get_second = itemgetter(1)
  get_topic = itemgetter('_id', 'probability')
  def topic_stat(it_tp):
    it_tp = tuple(it_tp)
    probabs = tuple(map(get_second, it_tp))
    return dict(
      count=len(it_tp),)
      # probability_avg=mean(probabs),
      # probability_pstdev=pstdev(probabs))
  get_ngr = itemgetter('_id', 'cnt')
  get_count = itemgetter('count')
  async for cont in contexts.aggregate(pipeline):
    conts = cont.pop('conts')

    cont_ids = map(itemgetter('cid'), conts)

    topics = chain.from_iterable(map(itemgetter('topics'), conts))
    # удалять топики < 0.5
    topics = ((t, p) for t, p in map(get_topic, topics) if p >= probability)
    topics = (
      dict(topic=t, **topic_stat(it_tp))
      for t, it_tp in groupby(sorted(topics, key=get_first), key=get_first))
    topics = sorted(topics, key=get_count, reverse=True)

    get_ngrs = lambda cont: cont.get('ngrams') or ()
    ngrams = chain.from_iterable(map(get_ngrs, conts))
    # только 2-grams и lemmas
    ngrams = (
      (n.split('_', 1)[-1].split(), c)
      for n, c in map(get_ngr, ngrams) if n.startswith('lemmas_'))
    ngrams = ((' '.join(n), c) for n, c in ngrams if len(n) == 2)
    ngrams = (
      dict(ngramm=n, count=sum(map(get_second, it_nc)))
      for n, it_nc in groupby(sorted(ngrams, key=get_first), key=get_first))
    ngrams = sorted(ngrams, key=get_count, reverse=True)
    ngrams = islice(ngrams, 10)
    cont.update(
      cont_ids=tuple(cont_ids),
      topics=tuple(topics),
      ngrams=tuple(ngrams))
    out_bund.append(cont)
  out = out_bund

  return json_response(out)


async def _ref_auth4ngramm_tops(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)
  probability = getreqarg_probability(request)

  contexts:Collection = mdb.contexts

  out_bund = []
  pipeline = [
    {'$match': {'exact': {'$exists': True}}}, {
    '$project': {'prefix': False, 'suffix': False, 'exact': False, }},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
      'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$unwind': '$bundle.authors'},
    {'$group': {
      '_id': '$bundle.authors', 'pubs': {'$addToSet': '$pub_id'},
      'conts': {'$addToSet': {
        'cid': '$_id', 'topics': '$linked_papers_topics',
        'ngrams': '$linked_papers_ngrams'}}}},
    {'$project': {
      '_id': False,
      'aurhor': '$_id', 'cits': {'$size': '$conts'}, 'pubs': {'$size': '$pubs'},
      'pubs_ids': '$pubs', 'conts': '$conts',
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title',}},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  get_probab = itemgetter('probability')
  get_first = itemgetter(0)
  get_second = itemgetter(1)
  get_topic = itemgetter('_id', 'probability')
  def topic_stat(it_tp):
    it_tp = tuple(it_tp)
    probabs = tuple(map(get_second, it_tp))
    return dict(
      count=len(it_tp),)
      # probability_avg=mean(probabs),
      # probability_pstdev=pstdev(probabs))
  get_ngr = itemgetter('_id', 'cnt')
  get_count = itemgetter('count')
  async for cont in contexts.aggregate(pipeline):
    conts = cont.pop('conts')

    cont_ids = map(itemgetter('cid'), conts)

    topics = chain.from_iterable(map(itemgetter('topics'), conts))
    # удалять топики < 0.5
    topics = ((t, p) for t, p in map(get_topic, topics) if p >= probability)
    topics = (
      dict(topic=t, **topic_stat(it_tp))
      for t, it_tp in groupby(sorted(topics, key=get_first), key=get_first))
    topics = sorted(topics, key=get_count, reverse=True)

    get_ngrs = lambda cont: cont.get('ngrams') or ()
    ngrams = chain.from_iterable(map(get_ngrs, conts))
    # только 2-grams и lemmas
    ngrams = (
      (n.split('_', 1)[-1].split(), c)
      for n, c in map(get_ngr, ngrams) if n.startswith('lemmas_'))
    ngrams = ((' '.join(n), c) for n, c in ngrams if len(n) == 2)
    ngrams = (
      dict(ngramm=n, count=sum(map(get_second, it_nc)))
      for n, it_nc in groupby(sorted(ngrams, key=get_first), key=get_first))
    ngrams = sorted(ngrams, key=get_count, reverse=True)
    ngrams = islice(ngrams, 10)
    cont.update(
      cont_ids=tuple(cont_ids),
      topics=tuple(topics),
      ngrams=tuple(ngrams))
    out_bund.append(cont)
  out = out_bund

  return json_response(out)


async def _req_pos_neg_pubs(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  contexts: Collection = mdb.contexts
  out = []
  async for doc in contexts.aggregate([
    {'$match': {'positive_negative': {'$exists': True}}, },
    {'$group': {
      '_id': {'pid': '$pub_id', 'pos_neg': '$positive_negative.val'},
      'cnt': {'$sum': 1}}},
    {'$group': {
      '_id': '$_id.pid',
      'pos_neg': {'$push': {'val': '$_id.pos_neg', 'cnt': '$cnt'}}}},
    {'$sort': {'_id': 1}},
    {'$lookup': {
      'from': 'publications', 'localField': '_id', 'foreignField': '_id',
      'as': 'pub'}},
    {'$unwind': '$pub'},
    {'$project': {'pos_neg': True, 'pub.name': True}}
  ]):
    pid:str = doc['_id']
    name:str = doc['pub']['name']
    classif = doc['pos_neg']
    neutral = sum(v['cnt'] for v in classif if v['val'] == 0)
    positive = sum(v['cnt'] for v in classif if v['val'] > 0)
    negative = sum(v['cnt'] for v in classif if v['val'] < 0)
    out.append(
      dict(
        pub=pid, name=name, neutral=int(neutral), positive=int(positive),
        negative=int(negative)))

  return json_response(out)


async def _req_pos_neg_refbundles(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refbindles_pipeline(topn)

  contexts:Collection = mdb.contexts
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

  return json_response(out)


async def _req_pos_neg_refauthors(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refauthors_pipeline(topn)

  contexts:Collection = mdb.contexts
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

  return json_response(out)


async def _req_frags_refbundles(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refbindles_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  async for doc in contexts.aggregate(pipeline):
    doc.pop('pos_neg', None)
    frags = Counter(doc.pop('frags', ()))
    doc.update(frags=frags)
    out.append(doc)

  return json_response(out)



async def _req_frags_neg_refauthors(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refauthors_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  async for row in contexts.aggregate(pipeline):
    row.pop('pos_neg', None)
    frags = Counter(row.pop('frags', ()))
    row.update(frags=frags)
    out.append(row)

  return json_response(out)


async def _req_top_ngramm_pubs(request: web.Request) -> web.StreamResponse:
  """Топ N фраз по публикациям"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = [
    {'$match': {'linked_papers_ngrams._id': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False}},
    {'$unwind': '$linked_papers_ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]

  nka = getreqarg_int(request, 'nka')
  ltype = getreqarg(request, 'ltype')

  if nka or ltype:
    pipeline = [
      {'$match': {
        f'ngrm.{f}': v for f, v in (('nka', nka), ('type', ltype)) if v}}]

  if ltype:
    gident = '$ngrm.title'
  else:
    gident = {'title': '$ngrm.title', 'type': '$ngrm.type'}

  pipeline += [
  {'$group': {
      '_id': gident, 'count': {'$sum': '$linked_papers_ngrams.cnt'},
      'conts': {
        '$push': {
          'pub_id': '$pub_id', 'cnt': '$linked_papers_ngrams.cnt'}}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]

  if topn:
    pipeline += [{'$limit': topn}]

  contexts = mdb.contexts
  get_as_tuple = itemgetter('_id', 'count', 'conts')
  topN = [get_as_tuple(obj) async for obj in contexts.aggregate(pipeline)]

  get_pubs = itemgetter('pub_id', 'cnt')
  if ltype:
    out = {
      name: dict(
        all=cnt, contects=Counter(
          p for p, n in (get_pubs(co) for co in conts)
          for _ in range(n)
        ))
      for name, cnt, conts in topN}
  else:
    out = [
      (name, dict(
        all=cnt, contects=Counter(
          p for p, n in (get_pubs(co) for co in conts)
          for _ in range(n)
        )))
      for name, cnt, conts in topN]
  return json_response(out)


async def _req_top_ngramm(request: web.Request) -> web.StreamResponse:
  """Топ N фраз по публикациям"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = [
    {'$match': {'linked_papers_ngrams._id': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False}},
    {'$unwind': '$linked_papers_ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]

  nka = getreqarg_int(request, 'nka')
  ltype = getreqarg(request, 'ltype')

  if nka or ltype:
    pipeline = [
      {'$match': {
        f'ngrm.{f}': v for f, v in (('nka', nka), ('type', ltype)) if v}}]

  if ltype:
    gident = '$ngrm.title'
  else:
    gident = {'title': '$ngrm.title', 'type': '$ngrm.type'}

  pipeline += [
  {'$group': {
      '_id': gident, 'count': {'$sum': '$linked_papers_ngrams.cnt'},
      'count_cont': {'$sum': 1},
      'conts': {
        '$push': {
          'cont_id': '$_id', 'cnt': '$linked_papers_ngrams.cnt'}}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]

  if topn:
    pipeline += [{'$limit': topn}]

  contexts = mdb.contexts
  get_as_tuple = itemgetter('_id', 'count', 'count_cont', 'conts')
  topN = [get_as_tuple(obj) async for obj in contexts.aggregate(pipeline)]

  get_pubs = itemgetter('cont_id', 'cnt')
  if ltype:
    out = {
      name: dict(
        all=cnt, count_cont=count_cont,
        contects=Counter(
          p for p, n in (get_pubs(co) for co in conts)
          for _ in range(n)
        ))
      for name, cnt, count_cont, conts in topN}
  else:
    out = [
      (name, dict(
        all=cnt, count_cont=count_cont,
        contects=Counter(
          p for p, n in (get_pubs(co) for co in conts)
          for _ in range(n)
        )))
      for name, cnt, count_cont, conts in topN]
  return json_response(out)


async def _req_publications(request: web.Request) -> web.StreamResponse:
  """Публикации"""
  app = request.app
  mdb = app['db']
  publications = mdb.publications
  out = [
    doc async for doc in publications.find(
      {'uni_authors': 'Sergey-Sinelnikov-Murylev'}
    ).sort([('year', ASCENDING), ('_id', ASCENDING)])]
  return json_response(out)


async def _req_top_topics(request: web.Request) -> web.StreamResponse:
  """Топ N топиков"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  probability = getreqarg(request, 'probability')
  probability = fast_float(probability, default=.5) if probability else .5

  pipeline = [
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_ngrams': False}},
    {'$unwind': '$linked_papers_topics'},
    {'$match': {'linked_papers_topics.probability': {'$gte': probability}}},
    {'$group': {
      '_id': '$linked_papers_topics._id', 'count': {'$sum': 1},
      'probability_avg': {'$avg': '$linked_papers_topics.probability'},
      'probability_stddev': {'$stdDevPop': '$linked_papers_topics.probability'},
      'conts': {'$addToSet': '$_id'}, }},
    {'$sort': {'count': -1, '_id': 1}}
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  to_out = lambda _id, count, probability_avg, probability_stddev, conts: dict(
    topic=_id, count=count, probability_avg=probability_avg,
    probability_stddev=probability_stddev,
    contects=conts)

  out = [to_out(**doc) async for doc in curs]
  return json_response(out)



async def _req_top_topics_pubs(request: web.Request) -> web.StreamResponse:
  """Топ N топиков"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  probability = getreqarg_probability(request)

  pipeline = [
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_ngrams': False}},
    {'$unwind': '$linked_papers_topics'},
    {'$match': {'linked_papers_topics.probability': {'$gte': probability}}},
    {'$group': {
      '_id': '$linked_papers_topics._id', 'count': {'$sum': 1},
      'probability_avg': {'$avg': '$linked_papers_topics.probability'},
      'probability_stddev': {'$stdDevPop': '$linked_papers_topics.probability'},
      'pubs': {'$addToSet': '$pub_id'}, }},
    {'$project': {
      'count_pubs': {'$size': '$pubs'}, 'count_conts': '$count',
      'probability_avg': 1, 'probability_stddev': 1, 'pubs': 1}},
    {'$sort': {'count_pubs': -1, 'count_conts': -1, '_id': 1}}
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  to_out = lambda _id, pubs, **kwds: dict(topic=_id, **kwds, publications=pubs)

  out = [to_out(**doc) async for doc in curs]
  return json_response(out)


if __name__ == '__main__':
  main()

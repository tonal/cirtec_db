#! /usr/bin/env python3
# -*- codong: utf-8 -*-
import asyncio
from functools import partial
import logging
from operator import itemgetter
from typing import Union

from aiohttp import web
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
  _req_top_ngramm, _req_top_ngramm_pubs, _req_top_topics, _req_top_topics_pubs,
  _reg_cnt_ngramm, _reg_cnt_pubs_ngramm)
from server_utils import getreqarg_topn, json_response, _init_logging
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
  add_get(
    '/cirtec_dev/ref_auth_bund4ngramm_tops/', _ref_auth_bund4ngramm_tops)
  add_get(
    '/cirtec_dev/ref_bund4ngramm_tops/', _ref_bund4ngramm_tops)
  add_get(
    '/cirtec_dev/ref_auth4ngramm_tops/', _ref_auth4ngramm_tops)

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
  out = [row async for row in contexts.aggregate(pipeline)]

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
      '_id': '$bundles', 'cits': {'$sum': 1},
      'pubs': {'$addToSet': '$pub_id'}}},
    {'$unwind': '$pubs'},
    {'$group': {
      '_id': '$_id', 'cits': {'$first': '$cits'}, 'pubs': {'$sum': 1}, }},
    # 'pubs_ids': {'$addToSet': '$pubs'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': '_id', 'foreignField': '_id',
      'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$project': {
      'cits': True, 'pubs': True, 'pubs_ids': True,
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title', }},
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
  out = [row async for row in contexts.aggregate(pipeline)]

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
            'total_pubs': '$bun.total_pubs'}}}},
    {'$project': {
      '_id': False, 'author': '$_id', 'cits': {'$size': '$cits'},
      'pubs': {'$size': '$pubs'}, 'total_cits': {'$sum': '$binds.total_cits'},
      'total_pubs': {'$sum': '$binds.total_pubs'}, }},
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
    ref_authors = [row async for row in contexts.aggregate(pub_pipeline)]
    pub_out = dict(pub_id=pid, name=pub['name'], ref_authors=ref_authors)
    out.append(pub_out)
  return json_response(out)


async def _ref_auth_bund4ngramm_tops(request: web.Request) -> web.StreamResponse:
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


async def _ref_bund4ngramm_tops(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  contexts:Collection = mdb.contexts

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

  out = out_bund

  return json_response(out)


async def _ref_auth4ngramm_tops(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  contexts:Collection = mdb.contexts

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

  out = out_acont

  return json_response(out)


if __name__ == '__main__':
  main()

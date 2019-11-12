#! /usr/bin/env python3
# -*- codong: utf-8 -*-
import asyncio
from functools import partial
import logging
from typing import Union

from aiohttp import web
import motor.motor_asyncio as aiomotor
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

  add_get(r'/cirtec/top/ref_bindles/', _req_top_refbindles)
  add_get(r'/cirtec/top/ref_authors/', _req_top_refauthors)

  app['conf'] = conf
  app['tasks'] = set()
  app.cleanup_ctx.append(_db_context)
  app.on_cleanup.append(_clean_tasks)

  return app, conf


def _add_old_reqs(add_get):
  # А Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций
  add_get(r'/cirtec/frags/', _req_frags)
  #   Распределение цитирований по 5-ти фрагментам для отдельных публикаций. #заданного автора.
  add_get(r'/cirtec/frags/publications/', _req_frags_pubs)
  #   Кросс-распределение «со-цитируемые авторы» по публикациям
  add_get(r'/cirtec/publ/publications/cocitauthors/',
    _req_publ_publications_cocitauthors)
  #   Кросс-распределение «фразы из контекстов цитирований» по публикациям
  add_get(r'/cirtec/publ/publications/ngramms/', _req_publ_publications_ngramms)
  #   Кросс-распределение «топики контекстов цитирований» по публикациям
  add_get(r'/cirtec/publ/publications/topics/', _req_publ_publications_topics)
  #   Распределение «со-цитируемые авторы» по 5-ти фрагментам
  add_get(r'/cirtec/frags/cocitauthors/', _req_frags_cocitauthors)
  #   Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»
  add_get(r'/cirtec/frags/cocitauthors/cocitauthors/',
    _req_frags_cocitauthors_cocitauthors)
  #   Кросс-распределение «публикации» - «со-цитируемые авторы»
  add_get(r'/cirtec/publ/cocitauthors/cocitauthors/',
    _req_publ_cocitauthors_cocitauthors)
  #   Распределение «5 фрагментов» - «фразы из контекстов цитирований»
  add_get(r'/cirtec/frags/ngramms/', _req_frags_ngramm)
  #   Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»
  add_get(r'/cirtec/frags/ngramms/ngramms/', _req_frags_ngramm_ngramm)
  #   Кросс-распределение «публикации» - «фразы из контекстов цитирований»
  add_get(r'/cirtec/publ/ngramms/ngramms/', _req_publ_ngramm_ngramm)
  #   Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»
  add_get(r'/cirtec/frags/topics/', _req_frags_topics)
  #   Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»
  add_get(r'/cirtec/frags/topics/topics/', _req_frags_topics_topics)
  #   Кросс-распределение «публикации» - «топики контекстов цитирований»
  add_get(r'/cirtec/publ/topics/topics/', _req_publ_topics_topics)
  # Б Кросс-распределение «со-цитирования» - «фразы из контекстов цитирований»
  add_get(r'/cirtec/frags/cocitauthors/ngramms/',
    _req_frags_cocitauthors_ngramms)
  #   Кросс-распределение «со-цитирования» - «топики контекстов цитирований»
  add_get(r'/cirtec/frags/cocitauthors/topics/', _req_frags_cocitauthors_topics)
  # В Кросс-распределение «фразы» - «со-цитирования»
  add_get(r'/cirtec/frags/ngramms/cocitauthors/',
    _req_frags_ngramms_cocitauthors)
  #   Кросс-распределение «фразы» - «топики контекстов цитирований»
  add_get(r'/cirtec/frags/ngramms/topics/', _req_frags_ngramms_topics)
  # Г Кросс-распределение «топики» - «со-цитирования»
  add_get(r'/cirtec/frags/topics/cocitauthors/', _req_frags_topics_cocitauthors)
  #   Кросс-распределение «топики» - «фразы»
  add_get(r'/cirtec/frags/topics/ngramms/', _req_frags_topics_ngramms)
  # Топ N со-цитируемых авторов
  add_get(r'/cirtec/top/cocitauthors/', _req_top_cocitauthors)
  # Топ N со-цитируемых авторов по публикациям
  add_get(r'/cirtec/top/cocitauthors/publications/', _req_top_cocitauthors_pubs)
  # Топ N фраз
  add_get(r'/cirtec/top/ngramms/', _req_top_ngramm)
  # Топ N фраз по публикациям
  add_get(r'/cirtec/top/ngramms/publications/', _req_top_ngramm_pubs)
  # Топ N топиков
  add_get(r'/cirtec/top/topics/', _req_top_topics)
  # Топ N топиков публикациям
  add_get(r'/cirtec/top/topics/publications/', _req_top_topics_pubs)
  add_get(r'/cirtec/cnt/ngramms/', _reg_cnt_ngramm)
  add_get(r'/cirtec/cnt/publications/ngramms/', _reg_cnt_pubs_ngramm)


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


async def _req_top_refbindles(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = [
    {'$match': {'exact': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False, 'linked_papers_ngrams': False}},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}}, ##
    {'$group': {
      '_id': '$bundles', 'cits': {'$sum': 1}, 'pubs': {'$addToSet': '$pub_id'}}},
    {'$unwind': '$pubs'},
    {'$group': {
      '_id': '$_id', 'cits': {'$first': '$cits'}, 'pubs': {'$sum': 1}, }}, # 'pubs_ids': {'$addToSet': '$pubs'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': '_id', 'foreignField': '_id',
      'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$project': {
      'cits': True, 'pubs': True, 'pubs_ids': True,
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title', }},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}},
    # {$count: 'cnt'}
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  contexts:Collection = mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]

  return json_response(out)


async def _req_top_refauthors(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

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
      'pubs': {'$addToSet': '$pub_id'}}},
    {'$project': {'cits': {'$size': '$cits'}, 'pubs': {'$size': '$pubs'}}},
    {'$sort': {'cits': -1, 'pubs': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  contexts:Collection = mdb.contexts
  out = [row async for row in contexts.aggregate(pipeline)]

  return json_response(out)


if __name__ == '__main__':
  main()

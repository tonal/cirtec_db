#! /usr/bin/env python3
# -*- codong: utf-8 -*-
import asyncio
from functools import partial
import logging
from typing import Union

from aiohttp import web
import motor.motor_asyncio as aiomotor
import uvloop

from server_requests import (
  _req_frags, _req_frags_pubs, _req_frags_cocitauthors,
  _req_publ_publications_cocitauthors, _req_publ_publications_ngramms,
  _req_publ_publications_topics, _req_frags_cocitauthors_cocitauthors,
  _req_publ_cocitauthors_cocitauthors, _req_frags_cocitauthors_ngramms,
  _req_frags_cocitauthors_topics, _req_top_cocitauthors,
  _req_top_cocitauthors_pubs, _req_frags_ngramm, _req_frags_ngramm_ngramm,
  _req_publ_ngramm_ngramm, _req_frags_ngramms_cocitauthors,
  _req_frags_ngramms_topics, _req_frags_topics, _req_frags_topics_topics,
  _req_publ_topics_topics, _req_frags_topics_cocitauthors,
  _req_frags_topics_ngramms, _reg_cnt_ngramm, _reg_cnt_pubs_ngramm)
from server_requests2 import (
  _req_top_refbundles, _req_top_refauthors, _req_pubs_refauthors,
  _req_auth_bund4ngramm_tops, _req_bund4ngramm_tops, _ref_auth4ngramm_tops,
  _req_pos_neg_pubs, _req_pos_neg_refbundles, _req_pos_neg_refauthors,
  _req_frags_refbundles, _req_frags_refauthors, _req_top_ngramm_pubs,
  _req_top_ngramm, _req_publications, _req_top_topics, _req_top_topics_pubs,
  _req_pos_neg_contexts, _req_pos_neg_ngramms, _req_pos_neg_topics,
  _req_pos_neg_cocitauthors, _req_frags_pos_neg_contexts,
  _req_frags_pos_neg_cocitauthors2, _req_top_cocitrefs, _req_top_cocitrefs2,
  _req_by_frags_refauthors)
from server_utils import _init_logging
from utils import load_config


_logger = logging.getLogger('cirtec')


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

  add_get('/cirtec/top/ref_bundles/', _req_top_refbundles)
  add_get('/cirtec/top/ref_authors/', _req_top_refauthors)
  add_get('/cirtec/pubs/ref_authors/', _req_pubs_refauthors)
  add_get('/cirtec/ref_auth_bund4ngramm_tops/', _req_auth_bund4ngramm_tops)
  add_get('/cirtec/ref_bund4ngramm_tops/', _req_bund4ngramm_tops)
  add_get('/cirtec/ref_auth4ngramm_tops/', _ref_auth4ngramm_tops)
  add_get('/cirtec/pos_neg/pubs/', _req_pos_neg_pubs)
  add_get('/cirtec/pos_neg/ref_bundles/', _req_pos_neg_refbundles)
  add_get('/cirtec/pos_neg/ref_authors/', _req_pos_neg_refauthors)
  add_get('/cirtec/frags/ref_bundles/', _req_frags_refbundles)
  add_get('/cirtec/frags/ref_authors/', _req_frags_refauthors)
  add_get('/cirtec/publications/', _req_publications)
  add_get('/cirtec/pos_neg/contexts/', _req_pos_neg_contexts)
  add_get('/cirtec/pos_neg/topics/', _req_pos_neg_topics)
  add_get('/cirtec/pos_neg/ngramms/', _req_pos_neg_ngramms)
  add_get('/cirtec/pos_neg/cocitauthors/', _req_pos_neg_cocitauthors)
  add_get('/cirtec/frags/pos_neg/contexts/', _req_frags_pos_neg_contexts)
  add_get(
    '/cirtec/frags/pos_neg/cocitauthors/cocitauthors/',
    _req_frags_pos_neg_cocitauthors2)
  # Топ N со-цитируемых авторов
  add_get(r'/cirtec/top/cocitrefs/', _req_top_cocitrefs)
  add_get(r'/cirtec/top/cocitrefs/cocitrefs/', _req_top_cocitrefs2)
  add_get('/cirtec/by_frags/ref_authors/', _req_by_frags_refauthors)


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
  add_get(
    r'/cirtec/publ/publications/cocitauthors/',
    _req_publ_publications_cocitauthors)
  #   Кросс-распределение «фразы из контекстов цитирований» по публикациям
  add_get(r'/cirtec/publ/publications/ngramms/', _req_publ_publications_ngramms)
  #   Кросс-распределение «топики контекстов цитирований» по публикациям
  add_get(r'/cirtec/publ/publications/topics/', _req_publ_publications_topics)

  #   Распределение «со-цитируемые авторы» по 5-ти фрагментам
  add_get(r'/cirtec/frags/cocitauthors/', _req_frags_cocitauthors)
  #   Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»
  add_get(
    r'/cirtec/frags/cocitauthors/cocitauthors/',
    _req_frags_cocitauthors_cocitauthors)
  #   Кросс-распределение «публикации» - «со-цитируемые авторы»
  add_get(
    r'/cirtec/publ/cocitauthors/cocitauthors/',
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
  add_get(
    r'/cirtec/frags/cocitauthors/ngramms/', _req_frags_cocitauthors_ngramms)
  #   Кросс-распределение «со-цитирования» - «топики контекстов цитирований»
  add_get(r'/cirtec/frags/cocitauthors/topics/', _req_frags_cocitauthors_topics)

  # В Кросс-распределение «фразы» - «со-цитирования»
  add_get(
    r'/cirtec/frags/ngramms/cocitauthors/', _req_frags_ngramms_cocitauthors)
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
  conf = load_config()

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


if __name__ == '__main__':
  main()

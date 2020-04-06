#! /usr/bin/env python3
# -*- codong: utf-8 -*-
import asyncio
from dataclasses import dataclass
from functools import partial
import logging
from typing import Union, Optional

from fastapi import APIRouter, FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.collection import Collection
from pymongo.database import Database
import uvicorn
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
  _req_top_refauthors, _req_pubs_refauthors,
  _req_auth_bund4ngramm_tops, _req_bund4ngramm_tops, _ref_auth4ngramm_tops,
  _req_pos_neg_pubs, _req_pos_neg_refbundles, _req_pos_neg_refauthors,
  _req_frags_refbundles, _req_frags_refauthors, _req_top_ngramm_pubs,
  _req_top_ngramm, _req_publications, _req_top_topics, _req_top_topics_pubs,
  _req_pos_neg_contexts, _req_pos_neg_ngramms, _req_pos_neg_topics,
  _req_pos_neg_cocitauthors, _req_frags_pos_neg_contexts,
  _req_frags_pos_neg_cocitauthors2, _req_top_cocitrefs, _req_top_cocitrefs2,
  _req_by_frags_refauthors, _req_top_detail_bund_refauthors, _req_contexts,
  _req_bundles, _req_pub_cont_bund,
  _get_refbindles_pipeline)
from server_utils import _init_logging
from utils import load_config


_logger = logging.getLogger('cirtec_dev_fastapi')


@dataclass(eq=False, order=False)
class Slot:
  conf:dict
  mdb:Database


slot:Optional[Slot] = None
router = APIRouter()


def main():
  # _init_logging()

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
    use_colors=True) # , log_config=None)


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


@router.get('/db/publication/',  # response_model=List[str],
  summary='Данные по указанному публикации (publications) из mongodb')
async def _db_publication(id: str):
  coll: Collection = slot.mdb.publications
  doc: dict = await coll.find_one(dict(_id=id))
  return doc


@router.get(
  '/db/bundle/', # response_model=List[str],
  summary='Данные по указанному бандлу (bundles) из mongodb')
async def _db_bundle(id:str):
  coll:Collection = slot.mdb.bundles
  doc:dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/db/context/',  # response_model=List[str],
  summary='Данные по указанному контексту (contexts) из mongodb')
async def _db_context(id: str):
  coll: Collection = slot.mdb.contexts
  doc: dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/db/topic/',  # response_model=List[str],
  summary='Данные по указанному топику (topics) из mongodb')
async def _db_topic(id: str):
  coll: Collection = slot.mdb.topics
  doc: dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/db/ngramm/',  # response_model=List[str],
  summary='Данные по указанной нграмме (n_gramms) из mongodb')
async def _db_topic(id: str):
  coll: Collection = slot.mdb.n_gramms
  doc: dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/top/ref_bundles/',  # response_model=List[str],
  summary='Топ N бандлов')
async def _top_ref_bundles(topn: int):

  coll: Collection = slot.mdb.contexts
  pipeline = _get_refbindles_pipeline(topn)
  out = []
  async for doc in coll.aggregate(pipeline):
    doc.pop('pos_neg', None)
    doc.pop('frags', None)
    out.append(doc)
  return out


def create_srv():
  conf = _load_conf()

  add_get = lambda *_: None

  add_get('/cirtec_dev/top/ref_authors/', _req_top_refauthors)
  add_get('/cirtec_dev/pubs/ref_authors/', _req_pubs_refauthors)
  add_get('/cirtec_dev/ref_auth_bund4ngramm_tops/', _req_auth_bund4ngramm_tops)
  add_get('/cirtec_dev/ref_bund4ngramm_tops/', _req_bund4ngramm_tops)
  add_get('/cirtec_dev/ref_auth4ngramm_tops/', _ref_auth4ngramm_tops)
  add_get('/cirtec_dev/pos_neg/pubs/', _req_pos_neg_pubs)
  add_get('/cirtec_dev/pos_neg/ref_bundles/', _req_pos_neg_refbundles)
  add_get('/cirtec_dev/pos_neg/ref_authors/', _req_pos_neg_refauthors)
  add_get('/cirtec_dev/frags/ref_bundles/', _req_frags_refbundles)
  add_get('/cirtec_dev/frags/ref_authors/', _req_frags_refauthors)
  add_get('/cirtec_dev/publications/', _req_publications)
  add_get('/cirtec_dev/pos_neg/contexts/', _req_pos_neg_contexts)
  add_get('/cirtec_dev/pos_neg/topics/', _req_pos_neg_topics)
  add_get('/cirtec_dev/pos_neg/ngramms/', _req_pos_neg_ngramms)
  add_get('/cirtec_dev/pos_neg/cocitauthors/', _req_pos_neg_cocitauthors)
  add_get('/cirtec_dev/frags/pos_neg/contexts/', _req_frags_pos_neg_contexts)
  add_get(
    '/cirtec_dev/frags/pos_neg/cocitauthors/cocitauthors/',
    _req_frags_pos_neg_cocitauthors2)
  # Топ N со-цитируемых авторов
  add_get(r'/cirtec_dev/top/cocitrefs/', _req_top_cocitrefs)
  add_get(r'/cirtec_dev/top/cocitrefs/cocitrefs/', _req_top_cocitrefs2)
  add_get('/cirtec_dev/by_frags/ref_authors/', _req_by_frags_refauthors)
  add_get(
    '/cirtec_dev/top_detail_bund/ref_authors/', _req_top_detail_bund_refauthors)

  add_get('/cirtec_dev/contexts/', _req_contexts)
  add_get('/cirtec_dev/bundles/', _req_bundles)
  add_get('/cirtec_dev/pub_cont_bund/', _req_pub_cont_bund)

  # def _add_old_reqs(add_get):
  # А Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций
  add_get(r'/cirtec_dev/frags/', _req_frags)
  #   Распределение цитирований по 5-ти фрагментам для отдельных публикаций. #заданного автора.
  add_get(r'/cirtec_dev/frags/publications/', _req_frags_pubs)
  #   Кросс-распределение «со-цитируемые авторы» по публикациям
  add_get(
    r'/cirtec_dev/publ/publications/cocitauthors/',
    _req_publ_publications_cocitauthors)
  #   Кросс-распределение «фразы из контекстов цитирований» по публикациям
  add_get(
    r'/cirtec_dev/publ/publications/ngramms/', _req_publ_publications_ngramms)
  #   Кросс-распределение «топики контекстов цитирований» по публикациям
  add_get(
    r'/cirtec_dev/publ/publications/topics/', _req_publ_publications_topics)
  #   Распределение «со-цитируемые авторы» по 5-ти фрагментам
  add_get(r'/cirtec_dev/frags/cocitauthors/', _req_frags_cocitauthors)
  #   Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»
  add_get(
    r'/cirtec_dev/frags/cocitauthors/cocitauthors/',
    _req_frags_cocitauthors_cocitauthors)
  #   Кросс-распределение «публикации» - «со-цитируемые авторы»
  add_get(
    r'/cirtec_dev/publ/cocitauthors/cocitauthors/',
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
  add_get(
    r'/cirtec_dev/frags/cocitauthors/ngramms/',
    _req_frags_cocitauthors_ngramms)
  #   Кросс-распределение «со-цитирования» - «топики контекстов цитирований»
  add_get(
    r'/cirtec_dev/frags/cocitauthors/topics/', _req_frags_cocitauthors_topics)
  # В Кросс-распределение «фразы» - «со-цитирования»
  add_get(
    r'/cirtec_dev/frags/ngramms/cocitauthors/',
    _req_frags_ngramms_cocitauthors)
  #   Кросс-распределение «фразы» - «топики контекстов цитирований»
  add_get(r'/cirtec_dev/frags/ngramms/topics/', _req_frags_ngramms_topics)
  # Г Кросс-распределение «топики» - «со-цитирования»
  add_get(
    r'/cirtec_dev/frags/topics/cocitauthors/', _req_frags_topics_cocitauthors)
  #   Кросс-распределение «топики» - «фразы»
  add_get(r'/cirtec_dev/frags/topics/ngramms/', _req_frags_topics_ngramms)
  # Топ N со-цитируемых авторов
  add_get(r'/cirtec_dev/top/cocitauthors/', _req_top_cocitauthors)
  # Топ N со-цитируемых авторов по публикациям
  add_get(
    r'/cirtec_dev/top/cocitauthors/publications/', _req_top_cocitauthors_pubs)
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


if __name__ == '__main__':
  main()

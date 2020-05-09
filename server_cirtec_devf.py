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

from server_dbquery import get_refauthors_pipeline, get_refbindles_pipeline
from utils import load_config


_logger = logging.getLogger('cirtec_dev_fastapi')


DEF_AUTHOR = 'Sergey-Sinelnikov-Murylev'


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
async def _top_ref_bundles(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, _add_pipeline:bool=False
):

  coll: Collection = slot.mdb.contexts
  pipeline = get_refbindles_pipeline(topn, author, cited, citing)
  out = []
  async for doc in coll.aggregate(pipeline):
    doc.pop('pos_neg', None)
    doc.pop('frags', None)
    out.append(doc)
  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/top/ref_authors/',  # response_model=List[str],
  summary='Топ N авторов бандлов')
async def _top_ref_bundles(topn: Optional[int] = None,
  author: Optional[str] = None, cited: Optional[str] = None,
  citing: Optional[str] = None, _add_pipeline: bool = False
):

  coll: Collection = slot.mdb.contexts
  pipeline = get_refauthors_pipeline(topn, author, cited, citing)
  out = []
  async for doc in coll.aggregate(pipeline):
    doc.pop('pos_neg', None)
    doc.pop('frags', None)
    out.append(doc)
  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


if __name__ == '__main__':
  main()

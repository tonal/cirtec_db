#! /usr/bin/env python3
# -*- codong: utf-8 -*-
from collections import Counter
from dataclasses import dataclass
from functools import partial
import logging
from operator import itemgetter
from typing import Optional

from fastapi import APIRouter, FastAPI, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
import uvicorn

from server_dbquery import (
  LType, filter_acc_dict, get_frag_publications, get_refauthors_pipeline,
  get_refbindles_pipeline, get_top_cocitauthors_pipeline,
  get_top_cocitrefs_pipeline, get_top_ngramms_pipeline, get_top_topics_pipeline)
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


@router.get('/db/publication/',
  summary='Данные по указанному публикации (publications) из mongodb')
async def _db_publication(id: str):
  coll: Collection = slot.mdb.publications
  doc: dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/db/bundle/',
  summary='Данные по указанному бандлу (bundles) из mongodb')
async def _db_bundle(id:str):
  coll:Collection = slot.mdb.bundles
  doc:dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/db/context/',
  summary='Данные по указанному контексту (contexts) из mongodb')
async def _db_context(id: str):
  coll: Collection = slot.mdb.contexts
  doc: dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/db/topic/',
  summary='Данные по указанному топику (topics) из mongodb')
async def _db_topic(id: str):
  coll: Collection = slot.mdb.topics
  doc: dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/db/ngramm/',
  summary='Данные по указанной нграмме (n_gramms) из mongodb')
async def _db_topic(id: str):
  coll: Collection = slot.mdb.n_gramms
  doc: dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/top/ref_bundles/',
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


@router.get('/top/ref_authors/',
  summary='Топ N авторов бандлов')
async def _top_ref_bundles(
  topn: Optional[int] = None,  author: Optional[str] = None,
  cited: Optional[str] = None, citing: Optional[str] = None,
  _add_pipeline: bool = False
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


@router.get('/top/topics/',
  summary='Топ N топиков')
async def _req_top_topics(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, probability:Optional[float]=.5,
  _add_pipeline:bool=False
):
  coll: Collection = slot.mdb.contexts
  pipeline = get_top_topics_pipeline(topn, author, cited, citing, probability)
  out = []
  async for doc in coll.aggregate(pipeline):
    doc.pop('pos_neg', None)
    doc.pop('frags', None)
    out.append(doc)
  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/top/ngramms/',
  summary='Топ N фраз по публикациям')
async def _req_top_topics(
  topn:Optional[int]=None, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None,
  nka:Optional[int]=Query(None, ge=0, le=6),
  ltype:Optional[LType]=Query(None, title='Тип фразы'), #, description='Может быть одно из значений "lemmas", "nolemmas" или пустой'),
  _add_pipeline:bool=False
):
  coll: Collection = slot.mdb.contexts
  pipeline = get_top_ngramms_pipeline(topn, author, cited, citing, nka, ltype)
  out = []
  get_as_tuple = itemgetter('_id', 'count', 'count_cont', 'conts')
  get_pubs = itemgetter('cont_id', 'cnt')
  key_sort = lambda kv: (-kv[-1], kv[0])
  get_name = itemgetter('title')
  get_ltype = itemgetter('type')

  async for doc in coll.aggregate(pipeline):
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
  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


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
  citing:Optional[str]=None, _add_pipeline:bool=False
):
  pass
  coll: Collection = slot.mdb.contexts
  pipeline = get_top_cocitrefs_pipeline(topn, author, cited, citing)

  def repack(_id, count, conts, bundles):
    authors = bundles.get('authors')
    title = bundles['title']
    year = bundles.get('year', '?')
    descr = f'{" ".join(authors) if authors else "?"} ({year}) {title}'
    return dict(bid=_id, descr=descr, contects=conts)

  out = [repack(**doc) async for doc in coll.aggregate(pipeline)]

  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/frags/publications/',
  summary='Распределение цитирований по 5-ти фрагментам для отдельных публикаций.')
async def _req_frags_pubs(
  author: Optional[str] = None, cited: Optional[str] = None,
  citing: Optional[str] = None,
  _add_pipeline: bool = False
):
  pass

  publications = slot.mdb.publications
  pipeline = get_frag_publications(author, cited, citing)

  to_tuple = itemgetter('_id', 'descr', 'sum', 'frags')
  out = []
  async for doc in publications.aggregate(pipeline):
    pubid, descr, sum, frags = to_tuple(doc)

    if len(frags) == 1 and 'fn' not in frags[0]:
      frags = {}
    else:
      frags = dict(map(itemgetter('fn', 'count'), frags))
    out.append(dict(pubid=pubid, descr=descr, sum=sum, frags=frags))

  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


@router.get('/pubs/ref_authors/',
) # summary='Топ N со-цитируемых референсов')
async def _req_pubs_refauthors(
  top_auth:Optional[int]=3, author:Optional[str]=None, cited:Optional[str]=None,
  citing:Optional[str]=None, _add_pipeline:bool=False
):
  publications: Collection = slot.mdb.publications
  contexts: Collection = slot.mdb.contexts

  pipeline = get_refauthors_pipeline(top_auth)

  out = []
  async for pub in publications.find(
    # {'uni_authors': 'Sergey-Sinelnikov-Murylev'},
    {'name': {'$exists': 1}, **filter_acc_dict(author, cited, citing),},
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

  if not _add_pipeline:
    return out

  return dict(pipeline=pipeline, items=out)


if __name__ == '__main__':
  main()

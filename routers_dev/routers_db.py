# -*- codong: utf-8 -*-
from functools import partial
from typing import List

from bson import ObjectId
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from pymongo.collection import Collection

from routers_dev.common import Slot
from server_utils import oid2dict


router = APIRouter()

_jsonable_encoder = partial(
  jsonable_encoder, custom_encoder={ObjectId: oid2dict})


@router.get('/bundle/', tags=['db'],
  summary='Данные по указанному бандлу (bundles) из mongodb')
async def _db_bundle(id:str, slot:Slot=Depends(Slot.req2slot)):
  # _logger.info('start func(%s)', id)
  coll:Collection = slot.mdb.bundles
  doc:dict = await coll.find_one(dict(_id=id))
  # _logger.info('end func(%s)->%s', id, doc)
  return doc


@router.post('/bundle/', tags=['db'],
  summary='Данные по указанным бандлам (bundles) из mongodb')
async def _db_bundle_lst(ids:List[str], slot:Slot=Depends(Slot.req2slot)):
  # _logger.info('start func(%s)', id)
  coll:Collection = slot.mdb.bundles
  curs = coll.find({'_id': {'$in': ids}}).sort('_id')
  out= [_jsonable_encoder(obj) async for obj in curs]
  return out


@router.get('/context/', tags=['db'],
  summary='Данные по указанному контексту (contexts) из mongodb')
async def _db_context(id: str, slot:Slot=Depends(Slot.req2slot)):
  coll:Collection = slot.mdb.contexts
  doc:dict = await coll.find_one(dict(_id=id))
  doc = _jsonable_encoder(doc)
  return doc


@router.post('/context/', tags=['db'],
  summary='Данные по указанным контекстам (contexts) из mongodb')
async def _db_context_lst(ids:List[str], slot:Slot=Depends(Slot.req2slot)):
  coll:Collection = slot.mdb.contexts
  curs = coll.find({'_id': {'$in': ids}}).sort('_id')
  out= [_jsonable_encoder(obj) async for obj in curs]
  return out


@router.get('/ngramm/', tags=['db'],
  summary='Данные по указанной нграмме (n_gramms) из mongodb')
async def _db_ngramm(id: str, slot:Slot=Depends(Slot.req2slot)):
  coll:Collection = slot.mdb.n_gramms
  doc:dict = await coll.find_one(dict(_id=id))
  return doc


@router.post('/ngramm/', tags=['db'],
  summary='Данные по указанным нграммам (n_gramms) из mongodb')
async def _db_ngramm_lst(ids:List[str], slot:Slot=Depends(Slot.req2slot)):
  coll:Collection = slot.mdb.n_gramms
  curs = coll.find({'_id': {'$in': ids}}).sort('_id')
  out= [_jsonable_encoder(obj) async for obj in curs]
  return out


@router.get('/publication/', tags=['db'],
  summary='Данные по указанному публикации (publications) из mongodb')
async def _db_publication(id: str, slot:Slot=Depends(Slot.req2slot)):
  coll:Collection = slot.mdb.publications
  doc:dict = await coll.find_one(dict(_id=id))
  return doc


@router.post('/publication/', tags=['db'],
  summary='Данные по указанным публикациям (publications) из mongodb')
async def _db_publication_lst(ids:List[str], slot:Slot=Depends(Slot.req2slot)):
  coll:Collection = slot.mdb.publications
  curs = coll.find({'_id': {'$in': ids}}).sort('_id')
  out= [_jsonable_encoder(obj) async for obj in curs]
  return out


@router.get('/db/topic/', tags=['db'],
  summary='Данные по указанному топику (topics) из mongodb')
async def _db_topic(id: str, slot:Slot=Depends(Slot.req2slot)):
  coll: Collection = slot.mdb.topics
  doc: dict = await coll.find_one(dict(_id=ObjectId(id)))
  if doc:
    doc = jsonable_encoder(doc, custom_encoder={ObjectId: oid2dict})
  return doc



@router.post('/db/topic/', tags=['db'],
  summary='Данные по указанным топикам (topics) из mongodb')
async def _db_topic_lst(ids:List[str], slot:Slot=Depends(Slot.req2slot)):
  coll: Collection = slot.mdb.topics
  # doc: dict = await coll.find_one(dict(_id=ObjectId(id)))
  curs = coll.find({'_id': {'$in': list(map(ObjectId, ids))}}).sort('_id')
  out= [_jsonable_encoder(obj) async for obj in curs]
  return out

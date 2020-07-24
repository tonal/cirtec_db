# -*- codong: utf-8 -*-
from bson import ObjectId
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from pymongo.collection import Collection

from routers_dev.common import Slot
from server_utils import oid2dict


router = APIRouter()


@router.get('/bundle/', tags=['db'],
  summary='Данные по указанному бандлу (bundles) из mongodb')
async def _db_bundle(id:str, slot:Slot=Depends(Slot.req2slot)):
  # _logger.info('start func(%s)', id)
  coll:Collection = slot.mdb.bundles
  doc:dict = await coll.find_one(dict(_id=id))
  # _logger.info('end func(%s)->%s', id, doc)
  return doc


@router.get('/context/', tags=['db'],
  summary='Данные по указанному контексту (contexts) из mongodb')
async def _db_context(id: str, slot:Slot=Depends(Slot.req2slot)):
  coll:Collection = slot.mdb.contexts
  doc:dict = await coll.find_one(dict(_id=id))
  doc = jsonable_encoder(doc, custom_encoder={ObjectId: oid2dict})
  return doc


@router.get('/ngramm/', tags=['db'],
  summary='Данные по указанной нграмме (n_gramms) из mongodb')
async def _db_ngramm(id: str, slot:Slot=Depends(Slot.req2slot)):
  coll:Collection = slot.mdb.n_gramms
  doc:dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/publication/', tags=['db'],
  summary='Данные по указанному публикации (publications) из mongodb')
async def _db_publication(id: str, slot:Slot=Depends(Slot.req2slot)):
  coll:Collection = slot.mdb.publications
  doc:dict = await coll.find_one(dict(_id=id))
  return doc


@router.get('/db/topic/', tags=['db'],
  summary='Данные по указанному топику (topics) из mongodb')
async def _db_topic(id: str, slot:Slot=Depends(Slot.req2slot)):
  coll: Collection = slot.mdb.topics
  doc: dict = await coll.find_one(dict(_id=ObjectId(id)))
  if doc:
    doc = jsonable_encoder(doc, custom_encoder={ObjectId: oid2dict})
  return doc

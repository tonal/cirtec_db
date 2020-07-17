# -*- codong: utf-8 -*-
from typing import Optional

from fastapi import APIRouter, Depends
from pymongo.collection import Collection

from models_dev.db_authors import get_publics
from models_dev.models import AType, NgrammParam
from routers_dev.common import (
  DebugOption, Slot, depNgrammParamReq)

router = APIRouter()


@router.get('/stat/',
  summary='Статистика по авторам')
async def _req_stat(
  atype:AType, ngrmpr:NgrammParam=Depends(depNgrammParamReq),
  probability:Optional[float]=.5,
  _debug_option:Optional[DebugOption]=None,
  slot:Slot=Depends(Slot.req2slot)
):
  # _logger.info('start func(%s)', id)
  pipeline = get_publics(atype, ngrmpr, probability)
  if _debug_option == DebugOption.pipeline:
    return pipeline
  coll:Collection = slot.mdb.publications
  curs = coll.aggregate(pipeline)
  if _debug_option == DebugOption.raw_out:
    return [doc async for doc in curs]

  out = []
  async for doc in curs:
    out.append(doc)

  return out

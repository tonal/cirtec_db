# -*- codong: utf-8 -*-
from dataclasses import dataclass, asdict as dc_asdict
import enum
from typing import ClassVar, Optional

from fastapi import Depends, Query, Request
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.database import Database

from models_dev.models import AuthorParam, LType, NgrammParam


DEF_AUTHOR = 'Sergey-Sinelnikov-Murylev'


@dataclass(eq=False, order=False)
class Slot:
  conf:dict
  mdb:Database

  slot: ClassVar[Optional['Slot']] = None

  @classmethod
  def init_slot(cls, conf:dict) -> 'Slot':
    mconf = conf['mongodb']
    mcli = AsyncIOMotorClient(mconf['uri'], compressors='zstd,snappy,zlib')
    mdb = mcli[mconf['db']] #.mail_links
    Slot.slot = slot = Slot(conf, mdb)
    return slot

  @classmethod
  def instance(cls) -> 'Slot':
    return cls.slot

  @classmethod
  def req2slot(cls, request: Request) -> 'Slot':
    return request.state.slot

  async def close(self):
    self.mdb.client.close()

  @classmethod
  def set2request(cls, request:Request):
    request.state.slot = cls.instance()


class DebugOption(str, enum.Enum):
  pipeline = 'pipeline'
  raw_out = 'raw_out'


def depNgrammParam(
  nka:Optional[int]=Query(None, ge=0, le=6),
  ltype:Optional[LType]=Query(
    None, title='Тип фразы',
    description='Тип фразы. Может быть одно из значений "lemmas", "nolemmas" или пустой')
):
  return NgrammParam(nka, ltype)


def depNgrammParamReq(
  nka:int=Query(..., ge=0, le=6),
  ltype:LType=Query(
    ...,
    title='Тип фразы',
    description='Тип фразы. Может быть одно из значений "lemmas", "nolemmas"')
):
  return NgrammParam(nka, ltype)


def depAuthorParamOnlyOne(authorParams:AuthorParam=Depends()):
  if not authorParams.only_one():
    raise ValueError(
      f'Должно быть заполнено только одно поле: {dc_asdict(authorParams)}')
  return authorParams
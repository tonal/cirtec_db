# -*- codong: utf-8 -*-
from dataclasses import dataclass
import enum
from typing import ClassVar, Optional

from fastapi import Depends, Query, Request
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, root_validator
from pymongo.database import Database

from models_dev.models import AuthorParam, LType, NgrammParam


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


class _OnlyOne(BaseModel):
  @root_validator(pre=True)
  def params_strip(cls, values):
    for k, v in values.items():
      newv = v.strip() if v else None
      if newv != v:
        values[k] = newv if newv else None
    return values

  @root_validator
  def check_only_one(cls, values):
    if sum(1 for k, v in values.items() if v) != 1:
      raise ValueError(
        f'Должно быть заполнено одно и только одно поле: {values}')
    return values


class AuthorParamOnlyOne(_OnlyOne):
  author: Optional[str]=None
  cited: Optional[str]=None
  citing: Optional[str]=None


def depAuthorParamOnlyOne(
  authorParams:AuthorParamOnlyOne=Depends()
) -> AuthorParam:
  res = AuthorParam(**authorParams.dict())
  return res


class AuthorParamOnlyOne2(_OnlyOne):
  author2:Optional[str]=None
  cited2:Optional[str]=None
  citing2:Optional[str]=None


def depAuthorParamOnlyOne2(
    ap:AuthorParamOnlyOne2=Depends()
) -> AuthorParam:
  params = AuthorParam(author=ap.author2, cited=ap.cited2, citing=ap.citing2)
  return params

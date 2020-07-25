# -*- codong: utf-8 -*-
from enum import Enum, auto
from typing import Optional, Tuple

from pydantic import BaseModel, conint, Field

import loads.common


DEF_AUTHOR = 'Sergey-Sinelnikov-Murylev'
ALL_AUTHORS = loads.common.AUTHORS
# (
#   'Sergey-Sinelnikov-Murylev',
#   'Alexander-Knobel',
#   'Alexander-Radygin',
#   'Alexandra-Bozhechkova',
#   'Andrey-Shastitko',
#   'Christopher-Baum',
#   'Maria-Kazakova',
#   'Natalia-Shagaida',
#   'Pavel-Trunin',
#   'Sergey-Drobyshevsky',
#   'Vasily-Uzun',
#   'Vladimir-Mau',
# )


class AutoName(Enum):
  def _generate_next_value_(name, start, count, last_values):
     return name


class AType(str, AutoName):
  author = auto()
  cited = auto()
  citing = auto()


Authors = Enum(
  'Authors',
  ((''.join(map(str.capitalize, a.split('-'))), a) for a in ALL_AUTHORS),
  type=str)


class AuthorParam(BaseModel):
  author:Optional[Authors]=None
  cited:Optional[Authors]=None
  citing:Optional[Authors]=None

  class Config:
    allow_mutation = False

  def is_empty(self):
    return not any((self.author, self.cited, self.citing))

  def only_one(self):
    return sum(1 for f in (self.author, self.cited, self.citing) if f) == 1

  def get_qual_auth(self) -> Tuple[AType, str]:
    atype, name = next((AType(k), v) for k, v in self.dict().items() if v)
    return atype, name.value


class LType(str, AutoName):
  lemmas = auto()
  nolemmas = auto()


class NgrammParam(BaseModel):
  nka:Optional[conint(ge=2, le=6)]=Field(None, title='Длина фразы')
  ltype:Optional[LType]=Field(None, title='Тип фразы')

  class Config:
    allow_mutation = False

  def is_empty(self):
    return not any([self.nka, self.ltype])

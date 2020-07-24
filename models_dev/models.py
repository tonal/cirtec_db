# -*- codong: utf-8 -*-
from dataclasses import dataclass, asdict as dc_asdict
from enum import Enum, auto
from typing import Optional, Tuple

DEF_AUTHOR = 'Sergey-Sinelnikov-Murylev'
ALL_AUTHORS = (
  'Sergey-Sinelnikov-Murylev',
  'Alexander-Knobel',
  'Alexander-Radygin',
  'Alexandra-Bozhechkova',
  'Andrey-Shastitko',
  'Christopher-Baum',
  'Maria-Kazakova',
  'Natalia-Shagaida',
  'Pavel-Trunin',
  'Sergey-Drobyshevsky',
  'Vasily-Uzun',
  'Vladimir-Mau',
)


class AutoName(Enum):
  def _generate_next_value_(name, start, count, last_values):
     return name


class AType(str, AutoName):
  author = auto()
  cited = auto()
  citing = auto()


@dataclass(order=False, frozen=True)
class AuthorParam:
  author:Optional[str]=None
  cited:Optional[str]=None
  citing:Optional[str]=None

  def is_empty(self):
    return not any((self.author, self.cited, self.citing))

  def only_one(self):
    return sum(1 for f in (self.author, self.cited, self.citing) if f) == 1

  def asdict(self):
    return dc_asdict(self)

  def get_qual_auth(self) -> Tuple[AType, str]:
    atype, name = next((AType(k), v) for k, v in self.asdict().items() if v)
    return atype, name


class LType(str, AutoName):
  lemmas = auto()
  nolemmas = auto()


@dataclass(eq=False, order=False, frozen=True)
class NgrammParam:
  nka:Optional[int]=None
  ltype:Optional[LType]=None

  def is_empty(self):
    return not any([self.nka, self.ltype])

  def asdict(self):
    return dc_asdict(self)

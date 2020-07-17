# -*- codong: utf-8 -*-
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class AutoName(Enum):
  def _generate_next_value_(name, start, count, last_values):
     return name


class AType(str, AutoName):
  author = auto()
  cited = auto()
  citing = auto()


@dataclass(eq=False, order=False, frozen=True)
class AuthorParam:
  author:Optional[str]=None
  cited:Optional[str]=None
  citing:Optional[str]=None

  def is_empty(self):
    return not any((self.author, self.cited, self.citing))

  def only_one(self):
    return sum(1 for f in (self.author, self.cited, self.citing) if f) == 1


class LType(str, AutoName):
  lemmas = auto()
  nolemmas = auto()


@dataclass(eq=False, order=False, frozen=True)
class NgrammParam:
  nka:Optional[int]=None
  ltype:Optional[LType]=None

  def is_empty(self):
    return not any([self.nka, self.ltype])

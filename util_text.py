# -*- codong: utf-8 -*-
from dataclasses import dataclass, field
from functools import partial
import re
from typing import Dict

import pymorphy2

re_drop_no_wrd = partial(re.compile(r'[^\w\d]+', re.I | re.U).sub, ' ')

LINK_CHAR = '☝'
LINK_FIRST_CHAR = '☞'
LINK_LAST_CHAR = '☜'

re_link = partial(re.compile(r'\[[^]]+\]').sub, LINK_CHAR)
re_link_first = partial(
  re.compile(r'^[^]]+\]\)?\.?').sub, LINK_FIRST_CHAR, count=1)
re_link_last = partial(re.compile(r'\(?\[[^]]+$').sub, LINK_LAST_CHAR, count=1)


@dataclass(eq=False)
class Text2Seq:
  wrd_norm:Dict[str, str] = field(default_factory=dict)
  morph:pymorphy2.MorphAnalyzer = field(default_factory=pymorphy2.MorphAnalyzer)

  def get_norm(self, w):
    wrd_norm = self.wrd_norm
    ow = (
      wrd_norm.get(w) or
      wrd_norm.setdefault(w, self.morph.parse(w)[0].normal_form))
    return ow

  def seq2text(self, text):
    seq = ' '.join(map(self.get_norm, re_drop_no_wrd(text).split()))
    return seq
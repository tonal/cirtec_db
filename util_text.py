# -*- codong: utf-8 -*-
from functools import partial
import re

re_drop_no_wrd = partial(re.compile(r'[^\w\d]+', re.I | re.U).sub, ' ')

LINK_CHAR = '☝'
LINK_FIRST_CHAR = '☞'
LINK_LAST_CHAR = '☜'

re_link = partial(re.compile(r'\[[^]]+\]').sub, LINK_CHAR)
re_link_first = partial(
  re.compile(r'^[^]]+\]\)?\.?').sub, LINK_FIRST_CHAR, count=1)
re_link_last = partial(re.compile(r'\(?\[[^]]+$').sub, LINK_LAST_CHAR, count=1)

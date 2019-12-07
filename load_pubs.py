#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка контекстов цитирования
"""
from collections import Counter
from datetime import datetime
from functools import partial, reduce
import re
from typing import Dict, List, Tuple
from urllib.parse import urlparse, parse_qs

from fastnumbers import fast_int
from pymongo.collection import Collection
from pymongo.database import Database
import requests
from lxml import etree
from parsel import Selector
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from utils import load_config, norm_spaces

SOURCE_XML = 'http://cirtec.ranepa.ru/prl/groups/Sergey-Sinelnikov-Murylev/xml/linked_papers.xml'
SOURCE_URL = 'http://cirtec.ranepa.ru/cgi/spadist4bundle.cgi?code=linked_papers&c=Sergey-Sinelnikov-Murylev'
AUTHOR = 'Sergey-Sinelnikov-Murylev'

re_start_stop = re.compile(
  r'start:\s* (?P<start>\d+)\s*;\s* end:\s* (?P<stop>\d+)', re.I | re.X
).match

re_prefix = re.compile(r'Prefix:\s* (.+)', re.I | re.X | re.S)
re_exact = re.compile(r'Exact:\s* (.+)', re.I | re.X | re.S)
re_suffix = re.compile(r'Suffix:\s* (.+)', re.I | re.X | re.S)


def main():
  start = datetime.now()
  conf = load_config()['dev']
  conf_mongo = conf['mongodb']
  linked_papers_xml:str = SOURCE_XML
  cont_url:str = SOURCE_URL
  for_del:int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb:Database = client[conf_mongo['db']]

    colls = update_pubs_conts(mdb, for_del, cont_url, linked_papers_xml)

    for coll in colls:
      r = coll.delete_many({'for_del': for_del})
      print(f'delete {coll.name}:', r.deleted_count)


def update_pubs_conts(
  mdb:Database, for_del:int, cont_url:str, linked_papers_xml:str
) -> Tuple[Collection, ...]:
  """Обновление публикаций и контекстов"""
  print('update_pubs_conts: Обновление публикаций и контекстов')
  mpubs:Collection = mdb['publications']
  mpubs_update = partial(mpubs.update_one, upsert=True)
  mcont:Collection = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)
  mpubs.update_many({}, {'$set': {'for_del': for_del}})
  mcont.update_many({}, {'$set': {'for_del': for_del}})
  pub_conts = load_url(cont_url)
  xml_root = etree.parse(linked_papers_xml)
  root = Selector(root=xml_root)
  for i, pub in enumerate(root.xpath('//ref'), 1):
    pub_id = pub.xpath('@found_in').get()
    pub_names = tuple(
      sorted(norm_spaces(t) for t in pub.xpath('citer/title/text()').getall()))

    authors = tuple(
      sorted(norm_spaces(a) for a in pub.xpath('citer/author/text()').getall()))

    year = norm_spaces(pub.xpath('citer/year/text()').get())

    pub_name = ' / '.join(pub_names)
    doc_pub = dict(
      name=pub_name, names=pub_names, uni_authors=[AUTHOR], reauthors=authors)
    if year:
      doc_pub.update(year=int(year))

    mpubs_update(dict(_id=pub_id), {'$set': doc_pub, '$unset': {'for_del': 1}})
    print(i, pub_id, pub_name)

    j = 0
    for j, (cont_id, cont) in enumerate(pub_conts[pub_id], 1):
      mcont_update(dict(_id=cont_id), {'$set': cont, '$unset': {'for_del': 1}})
    print(' ', j)
  return mcont, mpubs


def load_url(cont_url:str) -> Dict[str, List]:
  """Получение номера фрагмента для контекста"""
  re_prefix = re.compile(r'Prefix:\s* (.+)', re.I | re.X | re.S)
  re_exact = re.compile(r'Exact:\s* (.+)', re.I | re.X | re.S)
  re_suffix = re.compile(r'Suffix:\s* (.+)', re.I | re.X | re.S)

  pub_conts = {}

  rsp = requests.get(cont_url)
  root = Selector(rsp.text)
  for i, pub in enumerate(root.xpath('//ol/li[b/a]'), 1):
    pub_lnk = pub.xpath('b/a/@href').get()
    quer = urlparse(pub_lnk).query
    pub_id = parse_qs(quer)['h'][0]
    pub_conts[pub_id] = conts = []
    for j, frag in enumerate(pub.xpath('ul/li[b]'), 1):
      fpref, fnum = frag.xpath('b/text()').get().split('#')
      assert fpref.startswith('Fragment')
      fnum = fast_int(fnum, raise_on_invalid=True)
      for k, ref_kont in enumerate(frag.xpath('ul/li[div]')):
        start_stop = ref_kont.xpath('div[1]/text()').get()
        start, end = map(int,
          re_start_stop(start_stop.strip()).group('start', 'stop'))
        cont_id = f'{pub_id}@{start}'
        prefix = ref_kont.xpath('div[2]/text()').re_first(re_prefix)
        exact = ref_kont.xpath('div[3]/text()').re_first(re_exact)
        suffix = ref_kont.xpath('div[4]/text()').re_first(re_suffix)

        cont = dict(
          pub_id=pub_id, frag_num=fnum, start=start, end=end,
          prefix=prefix, exact=exact, suffix=suffix)
        conts.append((cont_id, cont))

  return pub_conts


if __name__ == '__main__':
  main()

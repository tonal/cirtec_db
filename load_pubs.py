#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка контекстов цитирования
"""
from collections import Counter
from functools import partial
import re
from typing import Dict, List
from urllib.parse import urlparse, parse_qs

from fastnumbers import fast_int
import requests
from lxml import etree
from parsel import Selector
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from utils import load_config, norm_spaces

SOURCE_XML = 'linked_papers.xml'
SOURCE_URL = 'http://cirtec.repec.org/cgi/spadist4bundle.cgi?code=linked_papers&c=Sergey-Sinelnikov-Murylev'
AUTHOR = 'Sergey-Sinelnikov-Murylev'

re_start_stop = re.compile(
  r'start:\s* (?P<start>\d+)\s*;\s* end:\s* (?P<stop>\d+)', re.I | re.X
).match

re_prefix = re.compile(r'Prefix:\s* (.+)', re.I | re.X | re.S)
re_exact = re.compile(r'Exact:\s* (.+)', re.I | re.X | re.S)
re_suffix = re.compile(r'Suffix:\s* (.+)', re.I | re.X | re.S)


def main():
  conf = load_config()
  conf_mongo = conf['mongodb_dev']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'
    mpubs = mdb['publications']
    mpubs_update = partial(mpubs.update_one, upsert=True)
    mpubs_insert = partial(mpubs.insert_one)
    mcont = mdb['contexts']
    mcont_update = partial(mcont.update_one, upsert=True)

    pub_conts = load_url()
    xml_root = etree.parse(SOURCE_XML)
    root = Selector(root=xml_root) #open(SOURCE_XML).read())
    ref_in_frags = Counter()
    for i, pub in enumerate(root.xpath('//ref'), 1):
      pub_id = pub.xpath('@found_in').get()
      pub_names = tuple(sorted(
        norm_spaces(t) for t in pub.xpath('citer/title/text()').getall()))

      authors = tuple(sorted(
        norm_spaces(a) for a in pub.xpath('citer/author/text()').getall()))

      year = norm_spaces(pub.xpath('citer/year/text()').get())

      pub_name = ' / '.join(pub_names)
      doc_pub = dict(
        name=pub_name, names=pub_names, uni_authors=[AUTHOR], reauthors=authors)
      if year:
        doc_pub.update(year=int(year))

      # refs = []
      # for ref in pub.xpath('reference'):
      #   ref_num = fast_int(ref.attrib['num'])
      #   ref_author = norm_spaces(ref.attrib['author'])
      #   ref_title = norm_spaces(ref.attrib['title'])
      #   doc_ref = dict(
      #     num=ref_num, author=ref_author, title=ref_title)
      #   ref_year = fast_int(norm_spaces(ref.attrib['year'], default=''))
      #   if ref_year:
      #     doc_ref.update(year=ref_year)
      #   ref_doi = ref.attrib.get('doi')
      #   if ref_doi:
      #     doc_ref.update(doi=ref_doi)
      #   ref_handle = ref.attrib.get('handle')
      #   if ref_handle:
      #     doc_ref.update(handle=ref_handle)
      #     rdoc_publ = dict(
      #       _id=ref_handle, year=ref_year, title=ref_title,
      #       ref_author=ref_author)
      #     if ref_doi:
      #       rdoc_publ.update(doi=ref_doi)
      #     try:
      #       mpubs_insert(rdoc_publ)
      #     except DuplicateKeyError:
      #       pass
      #
      #   refs.append(doc_ref)
      #
      # doc_pub.update(refs=refs)

      mpubs_update(dict(_id=pub_id), {'$set': doc_pub})
      print(i, pub_id, pub_name)

      for j, (cont_id, cont) in enumerate(pub_conts[pub_id], 1):
        mcont_update(dict(_id=cont_id), {'$set': cont})
      print(' ', j)
      # for j, ref_cont in enumerate(pub.xpath('intextref'), 1):
      #   print(etree.tounicode(ref_cont.root))
      #   prefix = ref_cont.xpath('Prefix/text()').get()
      #   suffix = ref_cont.xpath('Suffix/text()').get()
      #   exact = ref_cont.xpath('Exact/text()').get()
      #   start = ref_cont.xpath('Start/text()').get()
      #   start = fast_int(start, raise_on_invalid=True)
      #   end = ref_cont.xpath('End/text()').get()
      #   end = fast_int(end, raise_on_invalid=True)
      #   ref_num = ref_cont.xpath('Reference/text()').get()
      #   ref_num = fast_int(ref_num, raise_on_invalid=True)
      #   cont_id = f'{pub_id}@{start}'
      #   fnum = frag_dict[cont_id]
      #   print(' ', j, fnum)
      #   mcont_update(
      #     dict(_id=cont_id),
      #     {'$set': dict(
      #       pub_id=pub_id, frag_num=fnum, start=start, end=end, ref_num=ref_num,
      #       prefix=prefix, exact=exact, suffix=suffix)})


def load_url() -> Dict[str, List]:
  """Получение номера фрагмента для контекста"""
  re_prefix = re.compile(r'Prefix:\s* (.+)', re.I | re.X | re.S)
  re_exact = re.compile(r'Exact:\s* (.+)', re.I | re.X | re.S)
  re_suffix = re.compile(r'Suffix:\s* (.+)', re.I | re.X | re.S)

  pub_conts = {}

  rsp = requests.get(SOURCE_URL)
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

        # frag_dict[cont_id] = fnum

        cont = dict(
          pub_id=pub_id, frag_num=fnum, start=start, end=end,
          prefix=prefix, exact=exact, suffix=suffix)
        conts.append((cont_id, cont))

  return pub_conts



if __name__ == '__main__':
  main()

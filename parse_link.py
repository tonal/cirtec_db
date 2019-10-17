#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка контекстов цитирования
"""
from collections import Counter
from functools import partial
import re
from urllib.parse import urlparse, parse_qs

import requests
# from lxml import etree
from parsel import Selector
from pymongo import MongoClient

from utuls import load_config


# LINKS = 'http://cirtec.ranepa.ru/cgi/spadist4bundle.cgi?code=linked_papers&c=Sergey-Sinelnikov-Murylev'
LINKS = 'http://cirtec.repec.org/cgi/spadist4bundle.cgi?code=linked_papers&c=Sergey-Sinelnikov-Murylev'

re_start_stop = re.compile(
  r'start:\s* (?P<start>\d+)\s*;\s* end:\s* (?P<stop>\d+)', re.I | re.X
).match

re_prefix = re.compile(r'Prefix:\s* (.+)', re.I | re.X | re.S)
re_exact = re.compile(r'Exact:\s* (.+)', re.I | re.X | re.S)
re_suffix = re.compile(r'Suffix:\s* (.+)', re.I | re.X | re.S)


def main():
  conf = load_config()
  conf_mongo = conf['mongodb']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb = client[conf_mongo['db']] # 'cirtec'
    mpubs = mdb['publications']
    mpubs_update = partial(mpubs.update_one, upsert=True)
    mcont = mdb['contexts']
    mcont_update = partial(mcont.update_one, upsert=True)

    rsp = requests.get(LINKS)
    # root = etree.parse(LINKS)
    root = Selector(rsp.text)
    ref_in_frags = Counter()
    for i, pub in enumerate(root.xpath('//ol/li[b/a]'), 1):
      pub_lnk = pub.xpath('b/a/@href').get()
      quer = urlparse(pub_lnk).query
      pub_id = parse_qs(quer)['h'][0]
      pub_name = pub.xpath('b/a/text()').get().strip()

      mpubs_update(dict(_id=pub_id), {'$set': dict(name=pub_name)})
      print(i, pub_id, pub_name)

      for j, frag in enumerate(pub.xpath('ul/li[b]'), 1):
        fpref, fnum = frag.xpath('b/text()').get().split('#')
        assert fpref.startswith('Fragment')
        fnum = int(fnum)
        print(' ', j, fnum)
        for k, ref_kont in enumerate(frag.xpath('ul/li[div]')):
          start_stop = ref_kont.xpath('div[1]/text()').get()
          start, end = map(
            int, re_start_stop(start_stop.strip()).group('start', 'stop'))
          print('   ', k, 'start_stop:', start, end, start_stop.strip())
          ref_in_frags[fnum] += 1
          prefix = ref_kont.xpath('div[2]/text()').re_first(re_prefix)
          exact = ref_kont.xpath('div[3]/text()').re_first(re_exact)
          suffix = ref_kont.xpath('div[4]/text()').re_first(re_suffix)
          # print('      pref:', prefix)
          # print('      exct:', exact)
          # print('      suff:', suffix)
          mcont_update(
            dict(_id=f'{pub_id}@{start}'),
            {'$set': dict(
              pub_id=pub_id, frag_num=fnum, start=start, end=end,
              prefix=prefix, exact=exact, suffix=suffix)})


    msg = f'{"/".join(str(ref_in_frags[i]) for i in range(1, 6))}'
    print(msg)

    curs = mcont.aggregate([
      {'$match': {'frag_num': {'$gt': 0}}},
      {'$group': {'_id': '$frag_num', 'count': {'$sum': 1}}},
      {'$sort': {'_id': 1}}
    ])
    cnts = Counter({doc['_id']: int(doc["count"]) for doc in curs})
    msg2 = f'{"/".join(str(cnts[i]) for i in range(1, 6))}'
    print(msg2)


if __name__ == '__main__':
  main()

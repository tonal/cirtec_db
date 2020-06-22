#! /usr/bin/env python3
# -*- codong: utf-8 -*-
from operator import itemgetter
import urllib.parse

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
import requests

from utils import load_config


def main():
  conf = load_config()['dev']
  conf_mongo = conf['mongodb']
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb: Database = client[conf_mongo['db']]
    pubs:Collection = mdb.publications
    getid = itemgetter('_id')
    quote = urllib.parse.quote
    url_pref = 'http://onir2.ranepa.ru:8081/data'
    i = cnt = 0
    for i, obj in enumerate(
      pubs.find({}, projection=['_id'], sort=[('_id', 1)]),
      1
    ):
      pid:str = getid(obj)
      url1 = pid.replace("repec", "RePEc")
      *murls, turl = url1.split(':', 3)
      if ':' in turl or '/' in turl:
        turl = quote(turl, safe='').lower()
      url2 = '/'.join(murls + [quote(turl).lower()])
      url = f'{url_pref}/{url2}/summary.xml'
      if (rsp := requests.head(url)).status_code != 200:
        print(i, pid, url, rsp.status_code)
      else:
        cnt += 1
    print(i, cnt, i - cnt)


if __name__ == '__main__':
  main()

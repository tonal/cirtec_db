#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка контекстов цитирования
"""
from collections import defaultdict
from datetime import datetime
from functools import partial, reduce
import re
from typing import Tuple

from pymongo.collection import Collection
from pymongo.database import Database
from lxml import etree
from parsel import Selector
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from load_utils import best_bibs, bib2bib, rename_new_field
from utils import load_config, norm_spaces

SOURCE_XML = (
  (
    'uni_authors',
    'http://onir2.ranepa.ru:8081/prl/groups/Sergey-Sinelnikov-Murylev/xml/linked_papers.xml'),
  (
    'uni_cited',
    'http://onir2.ranepa.ru:8081/prl/groups/Sergey-Sinelnikov-Murylev/xml/cited_papers.xml'),
  (
    'uni_citing',
    'http://onir2.ranepa.ru:8081/prl/groups/Sergey-Sinelnikov-Murylev/xml/citing_papers.xml'),
)

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
  linked_papers_xml = SOURCE_XML
  for_del:int = reduce(lambda x, y: x * 100 + y, start.timetuple()[:6])
  with MongoClient(conf_mongo['uri'], compressors='snappy') as client:
    mdb:Database = client[conf_mongo['db']]

    colls = update_pubs_conts(mdb, for_del, linked_papers_xml[:1])

    for coll in colls:
      r = coll.delete_many({'for_del': for_del})
      print(f'delete {coll.name}:', r.deleted_count)


def update_pubs_conts(
  mdb:Database, for_del:int, pubs_files
) -> Tuple[Collection, ...]:
  """Обновление публикаций и контекстов"""
  now = datetime.now
  mpubs:Collection = mdb['publications']
  mpubs_update = partial(mpubs.update_one, upsert=True)
  mpubs_insert = mpubs.insert_one
  mbnds = mdb['bundles']
  mbnds_update = partial(mbnds.update_one, upsert=True)

  mcont:Collection = mdb['contexts']
  mcont_update = partial(mcont.update_one, upsert=True)

  mpubs.update_many({}, {'$set': {'for_del': for_del}})
  mbnds.update_many({}, {'$set': {'for_del': for_del}})
  mcont.update_many({}, {'$set': {'for_del': for_del}})

  cache_pubs = set()
  cache_conts = set()
  cnt_pub = cnt_cont = 0

  for uni_field, papers_xml in pubs_files:
    cp, cc = load_xml(
      mpubs_update, mbnds_update, mcont_update,
      papers_xml, uni_field, cache_pubs, cache_conts)
    cnt_pub += cp
    cnt_cont += cc

  # Интеллектуальное заполнение выходных данных из имеющихся
  best_bibs(mbnds, mbnds_update)
  calc_cocut_authors(mcont)
  calc_totals(mpubs, mbnds, mbnds_update)

  rename_new_field(mbnds, 'bibs')
  rename_new_field(mcont, 'bundles')

  print(cnt_pub, cnt_cont)
  return mcont, mbnds, mpubs


def load_xml(
  mpubs_update, mbnds_update, mcont_update, linked_papers_xml:str,
  uni_field:str, cache_pubs, cache_conts
):
  xml_root = etree.parse(linked_papers_xml)
  i = cont_cnt = 0
  for i, pub in enumerate(Selector(root=xml_root).xpath('//ref'), 1):
    pub_id = pub.xpath('@found_in').get()
    if pub_id in cache_pubs:
      mpubs_update(dict(_id=pub_id),
        {'$addToSet': {uni_field: AUTHOR}, '$unset': {'for_del': 1}})
      print(i, pub_id, 'Публикация уже в базе')
      continue

    cache_pubs.add(pub_id)

    futli = pub.xpath('@futli').get()

    # Минимальная позицыя референса считается окончанием статьи.
    start_refs = 2 ** 32
    refs = {}
    # Разбираем референсы
    for ref in pub.xpath('reference'):
      num = int(ref.xpath('@num').get())
      start = int(ref.xpath('@start').get())
      end = int(ref.xpath('@end').get())
      title = ref.xpath('@title').get()
      author = ref.xpath('@author').get()
      year = ref.xpath('@year').get()
      bundle: str = ref.xpath('@bundle').get()
      doi = ref.xpath('@doi').get()
      url = ref.xpath('@url').get()
      handle = ref.xpath('@handle').get()
      handle_sup = ref.xpath('@hhandle_sup').get()
      from_pdf = ref.xpath('from_pdf/text()').get()

      start_refs = min(start_refs, start)
      bib_bib = bib2bib(author, year, title)
      ref_doc = dict(num=num, **bib_bib)
      ref_doc.update(start=start, end=end, from_pdf=from_pdf)
      if bundle:
        ref_doc.update(bundle=bundle)
      if doi:
        ref_doc.update(doi=doi)
      refs[num] = ref_doc

      if bundle:
        bundle_doc = dict(**bib_bib)
        mbnds_update(dict(_id=bundle), {
          '$set': bundle_doc, '$addToSet': {'bibs_new': bib_bib},
          '$unset': {'for_del': 1}})

        for iref_inp in ref.xpath('all_intext_ref/intext_ref/text()'):
          iref = iref_inp.get().split('@', 1)[1]
          rpub_id, rstart = iref.rsplit('@', 1)
          if rpub_id not in cache_pubs:
            mpubs_update(dict(_id=rpub_id), {'$unset': {'for_del': 1}})
            # cache_pubs.add(rpub_id)
          if (iref, bundle) not in cache_conts:
            mcont_update(dict(_id=iref), {
              '$set': {'pubid': rpub_id, 'start': int(rstart)},
              '$addToSet': {'bundles_new': bundle}, '$unset': {'for_del': 1}})
            cache_conts.add((iref, bundle))

    pub_names = tuple(
      sorted(norm_spaces(t) for t in pub.xpath('citer/title/text()').getall()))
    authors = tuple(
      sorted(norm_spaces(a) for a in pub.xpath('citer/author/text()').getall()))
    year = norm_spaces(pub.xpath('citer/year/text()').get())
    pub_name = ' / '.join(pub_names)

    doc_pub = dict(name=pub_name, names=pub_names, reauthors=authors,
      futli=futli, refs=tuple(refs.values()))
    if year:
      doc_pub.update(year=int(year))

    mpubs_update(dict(_id=pub_id), {
      '$set': doc_pub, '$addToSet': {uni_field: AUTHOR},
      '$unset': {'for_del': 1}})
    print(i, pub_id, pub_name)

    # Разбираем контексты цитирования
    j = 0
    one_five = start_refs / 5
    for j, cont_elt in enumerate(pub.xpath('intextref'), 1):
      start = int(cont_elt.xpath('Start/text()').get())
      end = int(cont_elt.xpath('End/text()').get())
      prefix = cont_elt.xpath('Prefix/text()').get() or ''
      exact = cont_elt.xpath('Exact/text()').get()
      suffix = cont_elt.xpath('Suffix/text()').get() or ''

      if start_refs <= start:
        fnum = 5
      else:
        fnum = int(start / one_five) + 1

      cont_id = f'{pub_id}@{start}'

      cont_refs = []
      bundles = set()
      k = 0
      for k, ref in enumerate(cont_elt.xpath('Reference'), 1):
        num = int(ref.xpath('text()').get())
        start = int(ref.xpath('@start').get())
        end = int(ref.xpath('@end').get())
        exact = ref.xpath('@exact').get()
        cont_ref_doc = dict(num=num, start=start, end=end, exact=exact)
        try:
          bnd = refs[num].get('bundle')
        except:
          print(f'  !!! conts:{j},{k} {num=}')
          bnd = None
        if bnd:
          bundles.add(bnd)
          cont_ref_doc.update(bundle=bnd)
          cache_conts.add((cont_id, bnd))
        cont_refs.append(cont_ref_doc)

      cont = dict(pubid=pub_id, frag_num=fnum, start=start, end=end,
        prefix=prefix, exact=exact, suffix=suffix, refs=cont_refs)

      if len(bundles) == 1:
        b2doc = tuple(bundles)[0]
      else:
        b2doc = {'$each': tuple(sorted(bundles))}
      mcont_update(dict(_id=cont_id), {
        '$set': cont, '$addToSet': {'bundles_new': b2doc},
        '$unset': {'for_del': 1}})

    print(' ', j)
    cont_cnt += j
  return i, cont_cnt


def calc_cocut_authors(mcont: Collection):
  mcont_update = mcont.update_one
  pipeline = [
    {'$unwind': '$bundles_new'},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles_new', 'foreignField': '_id',
      'as': 'bund'}},
    {'$unwind': '$bund'},
    {'$unwind': '$bund.authors'},
    {'$group': {
      '_id': '$_id', 'cocit_authors': {'$addToSet': "$bund.authors"}, }},
    {'$sort': {'_id': 1}},
  ]
  for row in mcont.aggregate(pipeline):
    mcont_update(
      dict(_id=row['_id']),
      {
        '$set': {'cocit_authors': row['cocit_authors']},
        "$unset": {'cocit_refs': ""}})


def calc_totals(mpubs:Collection, mbnds:Collection, mbnds_update):
  for obj in mpubs.aggregate([
    {'$project': {'refs': 1}},
    {'$unwind': '$refs'},
    {'$match': {'refs.bundle': {'$exists': 1}}},
    {'$project': {'bundle': '$refs.bundle'}},
    {'$lookup': {
      'from': 'contexts', 'localField': 'bundle', 'foreignField': 'bundles',
      'as': 'cont'}},
    {'$unwind': {'path': '$cont', 'preserveNullAndEmptyArrays': True}},
    {'$project': {'bundle': 1, 'cont': '$cont._id'}},
    {'$group': {
      '_id': '$bundle', 'pubs': {'$addToSet': '$_id'},
      'conts': {'$addToSet': '$cont'}}},
  ]):
    total_cits = len(obj['conts'])
    total_pubs = len(obj['pubs'])
    mbnds.update_one(
      dict(_id=obj['_id']),
      {'$set': dict(total_pubs=total_pubs, total_cits=total_cits)})



if __name__ == '__main__':
  main()

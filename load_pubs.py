#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка контекстов цитирования
"""
from datetime import datetime
from functools import partial, reduce
import re
from typing import Any, Dict, Iterable, Tuple

from pymongo.collection import Collection
from pymongo.database import Database
from lxml import etree
from parsel import Selector
from pymongo import MongoClient

from load_utils import AUTHORS, rename_new_field
from utils import load_config, norm_spaces


SOURCE_XML = (
  (
    'uni_authors',
    'http://onir2.ranepa.ru:8081/prl/groups/%(author)s/xml/linked_papers.xml'),
  (
    'uni_cited',
    'http://onir2.ranepa.ru:8081/prl/groups/%(author)s/xml/cited_papers.xml'),
  (
    'uni_citing',
    'http://onir2.ranepa.ru:8081/prl/groups/%(author)s/xml/citing_papers.xml'),
)

# AUTHOR = 'Sergey-Sinelnikov-Murylev'

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

    # colls = update_pubs_conts(mdb, [AUTHOR], for_del, linked_papers_xml[:1])
    colls = update_pubs_conts(mdb, AUTHORS, for_del, linked_papers_xml[:1])

    for coll in colls:
      r = coll.delete_many({'for_del': for_del})
      print(f'delete {coll.name}:', r.deleted_count)


def update_pubs_conts(
  mdb:Database, authors:Iterable[str], for_del:int, pubs_files
) -> Tuple[Collection, ...]:
  """Обновление публикаций и контекстов"""
  now = datetime.now
  mpubs:Collection = mdb['publications']
  mpubs_update = partial(mpubs.update_one, upsert=True)
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

  for author in authors:
    for uni_field, papers_xml in pubs_files:
      cp, cc = load_xml(
        mpubs_update, mbnds_update, mcont_update,
        papers_xml, author, uni_field, cache_pubs, cache_conts)
      cnt_pub += cp
      cnt_cont += cc

  # Интеллектуальное заполнение выходных данных из имеющихся
  best_bibs(mbnds, mbnds_update)
  calc_cocut_authors(mcont)
  # calc_totals(mpubs, mbnds, mbnds_update)

  mpubs.update_many({}, {'$unset': {'pub_id': for_del}})

  rename_new_field(mbnds, 'bibs')
  rename_new_field(mcont, 'bundles')

  print(cnt_pub, cnt_cont)
  return mcont, mbnds, mpubs


def load_xml(
  mpubs_update, mbnds_update, mcont_update, linked_papers_xml:str,
  uni_author:str, uni_field:str, cache_pubs, cache_conts
):
  xml_uri = linked_papers_xml % dict(author=uni_author)
  print(xml_uri)
  xml_root = etree.parse(xml_uri)
  i = cont_cnt = 0
  for i, pub in enumerate(Selector(root=xml_root).xpath('//ref'), 1):
    pub_id = pub.xpath('@found_in').get()
    if pub_id in cache_pubs:
      mpubs_update(dict(_id=pub_id),
        {'$addToSet': {uni_field: uni_author}, '$unset': {'for_del': 1}})
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
      '$set': doc_pub, '$addToSet': {uni_field: uni_author},
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
        cont_ref_doc = dict(num=num)
        if start := ref.xpath('@start').get():
          cont_ref_doc.update(start=int(start))
        if end := ref.xpath('@end').get():
          cont_ref_doc.update(end=int(end))
        if exact := ref.xpath('@exact').get():
          cont_ref_doc.update(exact=exact)
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
  now = datetime.now
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
  print(now(), 'calc_cocut_authors start')
  mcont.update_many({}, {"$unset": {'cocit_refs': ""}})
  i = 0
  for i, row in enumerate(mcont.aggregate(pipeline), 1):
    mcont_update(
      dict(_id=row['_id']),
      {'$set': {'cocit_authors': row['cocit_authors']},})
    if i % 1000 == 0:
      print(now(), 'calc_cocut_authors', i)
  print(now(), 'calc_cocut_authors end', i)


def calc_totals(mpubs:Collection, mbnds:Collection, mbnds_update):
  now = datetime.now
  print(now(), 'calc_totals start')
  pipeline = [
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
  ]
  i = 0
  for i, obj in enumerate(mpubs.aggregate(pipeline), 1):
    total_cits = len(obj['conts'])
    total_pubs = len(obj['pubs'])
    mbnds.update_one(
      dict(_id=obj['_id']),
      {'$set': dict(total_pubs=total_pubs, total_cits=total_cits)})
    if i % 1000 == 0:
      print(now(), 'calc_totals', i)
  print(now(), 'calc_totals end', i)


def bib2bib(a:str, y:str, t:str):
  res = {}
  if authors := a.strip():
    res.update(authors=tuple(sorted(set(authors.split()))))
  if y:
    # Только первый год из возможных
    if year := y.strip()[:4].strip():
      res.update(year=year)
  if title := norm_spaces(t):
    res.update(title=title)
  return res


def best_bibs(mbnds:Collection, mbnds_update):
  """Интеллектуальное заполнение выходных данных из имеющихся"""
  now = datetime.now
  print(now(), 'best_bibs start')

  def bkey(b: Dict[str, Any]) -> Tuple[int, int, int, str, int, int, list, str]:
    lb = len(b)
    if title := b.get('title'):
      tu = 1 if title[0].isupper() else 0
      tl = len(title)
    else:
      tu, tl = -1, 0
    if authors := b.get('authors'):
      al = len(authors)
      aa = sum(len(a) for a in authors)
    else:
      al = aa = 0
    return lb, tu, tl, title, al, aa, authors, b.get('year')

  i = cnt = 0
  for i, bundle in enumerate(
    mbnds.find(
      # Выбираем обновлённые, у которых массив выходных длиннее 1
      {'for_del': {'$exists': 0}, 'bibs_new.1': {'$exists': 1}}),
    1
  ):
    # 'ss'.isu
    bibs = bundle['bibs_new']
    best = max(bibs, key=bkey)
    real_bib = {k: v for k, v in bundle.items() if
      v and k in {'title', 'authors', 'year'}}
    if real_bib != best:
      mbnds_update(dict(_id=bundle['_id']), {'$set': best})
      cnt += 1
      if cnt % 1000 == 0:
        print(now(), 'best_bibs', i, cnt)

  print(now(), 'best_bibs end', i, cnt)


if __name__ == '__main__':
  main()

# -*- codong: utf-8 -*-
from typing import Any, Dict, Tuple

from pymongo.collection import Collection

from utils import norm_spaces


def rename_new_field(mcoll:Collection, fld_name:str):

  # mcont.update_many(
  #   {'$or': [
  #     {'cocit_authors': {'$exists': True}},
  #     {'cocit_authors_new': {'$exists': True}}]},
  #   {
  #     '$rename': {
  #       'cocit_authors': 'cocit_authors_old',
  #       'cocit_authors_new': 'cocit_authors'},
  #     '$unset': {'cocit_authors_old': 1},
  #   })
  fld_name_old = f'{fld_name}_old'
  fld_name_new = f'{fld_name}_new'
  mcoll.update_many({fld_name: {'$exists': True}},
    {'$rename': {fld_name: fld_name_old}})
  mcoll.update_many({fld_name_new: {'$exists': True}},
    {'$rename': {fld_name_new: fld_name}})
  mcoll.update_many({fld_name_old: {'$exists': True}},
    {'$unset': {fld_name_old: 1}})


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
  # Интеллектуальное заполнение выходных данных из имеющихся
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

  for bundle in mbnds.find(
    # Выбираем обновлённые, у которых массив выходных длиннее 1
    {'for_del': {'$exists': 0}, 'bibs_new.1': {'$exists': 1}}):
    # 'ss'.isu
    bibs = bundle['bibs_new']
    best = max(bibs, key=bkey)
    real_bib = {k: v for k, v in bundle.items() if
      v and k in {'title', 'authors', 'year'}}
    if real_bib != best:
      mbnds_update(dict(_id=bundle['_id']), {'$set': best})

# -*- codong: utf-8 -*-

from pymongo.collection import Collection

AUTHORS = (
  'Sergey-Sinelnikov-Murylev',
  'Alexander-Knobel',
  'Alexander-Radygin',
  'Alexandra-Bozhechkova',
  'Andrey-Shastitko',
  'Christopher-Baum',
  'Maria-Kazakova',
  'Natalia-Shagaida',
  'Pavel-Trunin',
  'Sergey-Drobyshevsky',
  'Vasily-Uzun',
  'Vladimir-Mau',
)


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

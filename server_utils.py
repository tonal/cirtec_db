# -*- codong: utf-8 -*-
from functools import partial
import json
import logging
import logging.config
from pathlib import Path
from typing import Optional

from aiohttp import web
from fastnumbers import fast_float
import yaml

_logger = logging.getLogger('cirtec')


_dump = partial(json.dumps, ensure_ascii=False, check_circular=False)
json_response = partial(web.json_response, dumps=_dump)


def _init_logging():
  self_path = Path(__file__)
  conf_log_path = self_path.with_name('logging.yaml')
  conf_log = yaml.full_load(conf_log_path.open(encoding='utf-8'))
  logging.config.dictConfig(conf_log['logging'])
  # dsn = conf_log.get('sentry', {}).get('dsn')
  # if dsn:
  #   sentry_sdk.init(dsn=dsn, integrations=[AioHttpIntegration()])


def getreqarg(request:web.Request, argname:str) -> Optional[str]:
  arg = request.query.get(argname)
  if arg:
    arg = arg.strip()
    return arg


def getreqarg_int(request:web.Request, argname:str) -> Optional[int]:
  arg = getreqarg(request, argname)
  if arg:
    try:
      arg = int(arg)
      return arg
    except ValueError as ex:
      _logger.error(
        'Неожиданное значение параметра topn "%s" при переводе в число: %s',
        arg, ex)


def getreqarg_topn(request: web.Request, *, default:int=None) -> Optional[int]:
  topn = getreqarg_int(request, 'topn')
  if not topn and default:
    return default
  return topn


def getreqarg_probability(request, default:float=.5) -> float:
  probab = getreqarg(request, 'probability')
  probability = (
    fast_float(probab, default=default) if probab else default)
  return probability


def getreqarg_nka(request) -> Optional[int]:
  nka = getreqarg_int(request, 'nka')
  return nka


def getreqarg_ltype(request) -> Optional[str]:
  ltype = getreqarg(request, 'ltype')
  return ltype


def getreqarg_id(request: web.Request) -> Optional[str]:
  id_ = getreqarg(request, 'id')
  return id_


def to_out_typed(_id, *, type:str, **kwds):
  return dict(_id=_id, type=type, **kwds)


def cvt_oid(_id, **kwd):
  # bson.json_util.default (732)
  # {"$oid": str(obj)}
  return {'_id': {"$oid": str(_id)}, **kwd}


def oid2dict(oid):
  # bson.json_util.default (732)
  return {"$oid": str(oid)}

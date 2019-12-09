# -*- codong: utf-8 -*-
from functools import partial
import json
import logging
import logging.config
from pathlib import Path
from typing import Optional

from aiohttp import web
import yaml

_logger = logging.getLogger('cirtec')


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


def getreqarg_topn(request: web.Request) -> Optional[int]:
  topn = getreqarg_int(request, 'topn')
  return topn


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

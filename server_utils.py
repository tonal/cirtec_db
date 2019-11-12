# -*- codong: utf-8 -*-
import logging
from typing import Optional

from aiohttp import web


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

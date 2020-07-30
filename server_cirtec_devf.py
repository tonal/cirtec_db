#! /usr/bin/env python3
# -*- codong: utf-8 -*-
import logging
from typing import Optional

from fastapi import FastAPI, Request
import uvicorn
from fastapi.exception_handlers import request_validation_exception_handler
from pydantic import ValidationError

from routers_dev.common import Slot
from routers_dev import (
  routers_author, routers_db, routers_frags, routers_misc, routers_posneg,
  routers_publ, routers_top)
from server_utils import _init_logging
from utils import get_logger_dev as get_logger, load_config_dev as load_config


_logger = get_logger()


def main():
  _init_logging()

  # app, conf = create_srv()
  # srv_run_args = conf['srv_run_args']
  # web.run_app(app, **srv_run_args)
  cummon_prefix = '/cirtec_dev'

  app = FastAPI(
    openapi_url=cummon_prefix + '/openapi.json',
    docs_url=cummon_prefix + '/docs',
    redoc_url=cummon_prefix + '/redoc',
    description='Сервер данных.'
  )

  conf = _load_conf()
  slot:Optional[Slot] = None

  # router.add_event_handler('startup', partial(Slot.init_slot, conf))
  # app.on_event('startup')(partial(Slot.init_slot, conf))
  @app.on_event('startup')
  async def _():
    nonlocal conf, slot
    slot = Slot.init_slot(conf)

  @app.on_event('shutdown')
  async def _close_app():
    nonlocal slot
    await slot.close()

  app.include_router(routers_db.router, prefix=cummon_prefix + '/db')
  app.include_router(routers_author.router, prefix=cummon_prefix + '/authors')
  app.include_router(routers_frags.router, prefix=cummon_prefix + '/frags')
  app.include_router(routers_posneg.router, prefix=cummon_prefix + '/pos_neg')
  app.include_router(routers_publ.router, prefix=cummon_prefix + '/publ')
  app.include_router(routers_top.router, prefix=cummon_prefix + '/top')
  app.include_router(routers_misc.router, prefix=cummon_prefix)

  @app.middleware("http")
  async def db_session_middleware(request:Request, call_next):
    # request.state.slot = Slot.instance()
    Slot.set2request(request)
    response = await call_next(request)
    return response

  @app.exception_handler(ValidationError)
  async def ex_hdlr(request, exc):
    return await request_validation_exception_handler(request, exc)

  # asgi_app = SentryAsgiMiddleware(app)

  conf_app = conf['srv_run_args']
  uvicorn.run(
    app, host=conf_app.get('host') or '0.0.0.0',
    port=conf_app.get('port') or 8668,
    use_colors=True, log_config=None)


def _load_conf() -> dict:
  # env.read_envfile()
  conf = load_config()

  return conf


if __name__ == '__main__':
  main()

# -*- codong: utf-8 -*-
import asyncio
from collections import Counter, defaultdict
from functools import partial
import json
import logging
import logging.config
from operator import itemgetter
from pathlib import Path
from typing import Union, Optional, Sequence, Tuple

from aiohttp import web
import motor.motor_asyncio as aiomotor
import uvloop
import yaml

from utils import load_config

_logger = logging.getLogger('cirtec')


_dump = partial(json.dumps, ensure_ascii=False, check_circular=False)
json_response = partial(web.json_response, dumps=_dump)


def main():
  uvloop.install()

  _init_logging()

  app, conf = create_srv()

  srv_run_args = conf['srv_run_args']
  web.run_app(app, **srv_run_args)


def _init_logging():
  self_path = Path(__file__)
  conf_log_path = self_path.with_name('logging.yaml')
  conf_log = yaml.full_load(conf_log_path.open(encoding='utf-8'))
  logging.config.dictConfig(conf_log['logging'])
  # dsn = conf_log.get('sentry', {}).get('dsn')
  # if dsn:
  #   sentry_sdk.init(dsn=dsn, integrations=[AioHttpIntegration()])


def create_srv():
  conf = _load_conf()

  app = web.Application(middlewares=[error_pages])
  router = app.router
  add_get = partial(router.add_get, allow_head=False)

  # А Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций
  add_get(r'/cirtec/frags/', _req_frags)
  #   Распределение цитирований по 5-ти фрагментам для отдельных публикаций. #заданного автора.
  add_get(r'/cirtec/freq_contexts_by_pubs/', _req_freq_contexts_by_pubs)
  #   Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»
  add_get(r'/cirtec/freq_cocitauth_by_frags/', _req_freq_cocitauth_by_frags)
  #   Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»
  add_get(r'/cirtec/freq_ngramm_by_frag/', _req_freq_ngramm_by_frag)
  #   Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»
  add_get(r'/cirtec/freq_topics_by_frags/', _req_freq_topics_by_frags)

  # Топ N со-цитируемых авторов
  add_get(r'/cirtec/top_cocit_authors/', _req_top_cocit_authors)
  # Топ N фраз
  add_get(r'/cirtec/top_ngramm/', _req_top_ngramm)
  # Топ N топиков
  add_get(r'/cirtec/top_topics/', _req_top_topics)

  app['conf'] = conf
  app['tasks'] = set()
  app.cleanup_ctx.append(_db_context)
  app.on_cleanup.append(_clean_tasks)

  return app, conf


def _load_conf() -> dict:
  # env.read_envfile()
  conf = load_config()

  # conf['clickhouse'] = dict(
  #   url=env('CLICK_URL'), user=env('CLICK_USER', default='default'),
  #   password=env('CLICK_PASSWORD', default=None),
  #   database=env('CLICK_DB', default=None),
  # )

  return conf


@web.middleware
async def error_pages(request:web.Request, handler:callable):
  try:
    response = await handler(request)
    if response and response.status >= 500:
      await handle_500(request, response)
    return response
  except web.HTTPException as ex:
    if ex.status >= 500:
      await handle_500(request, ex)
    raise
  # except Can
  except Exception as ex:
    await handle_500(request, ex)
    raise


async def handle_500(
  request:web.Request, response:Union[web.Response, Exception]
):
  exc_info = isinstance(response, Exception)
  text = (await request.text()).strip()
  headers = '\n\t'.join(f'{k}: {v}' for k, v in request.headers.items())
  if text:
    text = '\n\t'.join(text.splitlines())
    text = f'\nДанные:\n\t{text}'
  _logger.error(
    f'Ошибка при обработке запроса: {request.method} {request.rel_url}\n'
    f'Заголовки: \n\t{headers}{text}',
    exc_info=exc_info)


async def _db_context(app):
  conf = app['conf']['mongodb']

  conn = aiomotor.AsyncIOMotorClient(conf['uri'], compressors='snappy') # type_registry=decimal_type_registry
  db = conn[conf['db']]
  app['db'] = db

  yield

  conn.close()


async def _clean_tasks(app):
  tasks = app['tasks']
  canceled = set()
  for t in tasks:
    # type: t:asyncio.Task
    if not t.done():
      t.cancel()
      canceled.add(t)
  if canceled:
    await asyncio.wait(canceled, return_when='ALL_COMPLETED')


async def _req_frags(request:web.Request) -> web.StreamResponse:
  """
  А
  Суммарное распределение цитирований по 5-ти фрагментам для всех публикаций
  """
  app = request.app
  mdb = app['db']
  contexts = mdb.contexts
  curs = contexts.aggregate([
    {'$match': {'frag_num': {'$gt': 0}}},
    {'$group': {'_id': '$frag_num', 'count': {'$sum': 1}}},
    {'$sort': {'_id': 1}}])
  cnts = Counter({doc['_id']: int(doc["count"]) async for doc in curs})
  return json_response(cnts)


async def _req_freq_contexts_by_pubs(
  request: web.Request
) -> web.StreamResponse:
  """
  А
  Распределение цитирований по 5-ти фрагментам для отдельных публикаций. #заданного автора.
  """
  app = request.app
  mdb = app['db']
  publications = mdb.publications
  pubs = {
    pdoc['_id']: (pdoc['name'], Counter())
    async for pdoc in publications.find({'name': {'$exists': True}})
  }
  contexts = mdb.contexts
  async for doc in contexts.aggregate([
    {'$match': {'frag_num': {'$gt': 0}}},
    {'$group': {
      '_id': {'fn': '$frag_num', 'pub_id': '$pub_id'}, 'count': {'$sum': 1}}},
    {'$sort': {'_id': 1}}
  ]):
    did = doc['_id']
    pub_id = did['pub_id']
    frn = did['fn']
    pubs[pub_id][1][frn] += int(doc['count'])

  out_pubs = {}
  for i, (pid, pub) in enumerate(
    sorted(pubs.items(), key=lambda kv: sum(kv[1][1].values()), reverse=True),
    1
  ):
    pcnts = pub[1]
    out_pubs[pid] = dict(descr=pub[0], sum=sum(pcnts.values()), frags=pcnts)

  return json_response(out_pubs)


async def _req_freq_cocitauth_by_frags(
  request: web.Request
) -> web.StreamResponse:
  """
  А
  Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»
  """
  app = request.app
  mdb = app['db']

  topn = _get_arg_topn(request)

  contexts = mdb.contexts
  topN = await _get_topn_cocit_authors(contexts, topn)

  out_dict = {}
  for i, (cocitauthor, cnt) in enumerate(topN, 1):
    frags = Counter()
    coaut = defaultdict(Counter)

    async for doc in contexts.find(
      {'cocit_authors': cocitauthor, 'frag_num': {'$gt': 0}},
      projection=['frag_num', 'cocit_authors']
    ).sort('frag_num'):
      # print(i, doc)
      fnum = doc['frag_num']
      coauthors = frozenset(
        c for c in doc['cocit_authors'] if c != cocitauthor)
      frags[fnum] += 1
      for ca in coauthors:
        coaut[ca][fnum] += 1

    out_cocitauthors = {}
    out_dict[cocitauthor] = dict(
      sum=cnt, frags=frags, cocitauthors=out_cocitauthors)

    for j, (co, cnts) in enumerate(
      sorted(coaut.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      out_cocitauthors[co] = dict(frags=cnts, sum=sum(cnts.values()))

  return json_response(out_dict)


async def _get_topn_cocit_authors(
  contexts, topn:Optional[int], *, include_conts:bool=False
):
  """Получить самых со-цитируемых авторов"""
  if include_conts:
    group = {
      '$group': {
        '_id': '$cocit_authors', 'count': {'$sum': 1},
        'conts': {'$addToSet': '$_id'}}}
    get_as_tuple = itemgetter('_id', 'count', 'conts')
  else:
    group = {'$group': {'_id': '$cocit_authors', 'count': {'$sum': 1}}}
    get_as_tuple = itemgetter('_id', 'count')

  pipe = [
    {'$match': {'frag_num': {'$gt': 0}, 'cocit_authors': {'$exists': True}}},
    {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
    {'$unwind': '$cocit_authors'},
    # {'$group': {'_id': '$cocit_authors', 'count': {'$sum': 1}}},
    group,
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipe += [{'$limit': topn}]

  top50 = [get_as_tuple(doc) async for doc in contexts.aggregate(pipe)]
  return tuple(top50)


async def _req_top_cocit_authors(request: web.Request) -> web.StreamResponse:
  """Топ N со-цитируемых авторов"""
  app = request.app
  mdb = app['db']

  topn = _get_arg_topn(request)

  contexts = mdb.contexts
  topN = await _get_topn_cocit_authors(contexts, topn, include_conts=True)
  out = tuple(dict(title=n, contects=conts) for n, _, conts in topN)

  return json_response(out)


def _get_arg(request:web.Request, argname:str) -> Optional[str]:
  arg = request.query.get(argname)
  if arg:
    arg = arg.strip()
    return arg


def _get_arg_int(request:web.Request, argname:str) -> Optional[int]:
  arg = _get_arg(request, argname)
  if arg:
    try:
      arg = int(arg)
      return arg
    except ValueError as ex:
      _logger.error(
        'Неожиданное значение параметра topn "%s" при переводе в число: %s',
        arg, ex)


def _get_arg_topn(request: web.Request) -> Optional[int]:
  topn = _get_arg_int(request, 'topn')
  return topn


async def _req_freq_ngramm_by_frag(request: web.Request) -> web.StreamResponse:
  """Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»"""
  app = request.app
  mdb = app['db']

  topn = _get_arg_topn(request)
  if not topn:
    topn = 10
  # nka: int = 2, ltype:str = 'lemmas'
  nka = _get_arg_int(request, 'nka')
  ltype = _get_arg(request, 'ltype')
  if nka or ltype:
    preselect = [
      {'$match': {f: v for f, v in (('nka', nka), ('type', ltype)) if v}}]
    postmath = [
      {'$match': {
        f: v for f, v in (('cont.nka', nka), ('cont.type', ltype)) if v}}]
  else:
    preselect = None
    postmath = None

  n_gramms = mdb.n_gramms
  topN = await _get_topn(
    n_gramms, topn, preselect=preselect, sum_expr='$linked_papers.cnt')
  exists = frozenset(t for t, _, _ in topN)
  out_dict = {}
  contexts = mdb.contexts

  pipeline = [
    {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
    {
      '$lookup': {
        'from': 'n_gramms', 'localField': '_id',
        'foreignField': 'linked_papers.cont_id', 'as': 'cont'}},
    {'$unwind': '$cont'},
  ]
  if postmath:
    pipeline += postmath
  pipeline += [
    {'$unwind': '$cont.linked_papers'},
    {'$match': {'$expr': {'$eq': ["$_id", "$cont.linked_papers.cont_id"]}}},
    {'$project': {'cont.type': False}},  # 'cont.linked_papers': False,
    # {'$sort': {'frag_num': 1}},
  ]
  for i, (ngrmm, cnt, conts) in enumerate(topN, 1):
    congr = defaultdict(Counter)

    work_pipeline = [
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}}
    ] + pipeline

    async for doc in contexts.aggregate(work_pipeline):
      cont = doc['cont']
      ngr = cont['title']
      if ngr not in exists:
        continue
      fnum = doc['frag_num']
      congr[ngr][fnum] += cont['linked_papers']['cnt']

    frags = congr.pop(ngrmm)
    crossgrams = {}
    out_dict[ngrmm] = dict(sum=cnt, frags=frags, crossgrams=crossgrams)

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crossgrams[co] = dict(frags=cnts, sum=sum(cnts.values()))

  return json_response(out_dict)


async def _req_freq_topics_by_frags(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = _get_arg_topn(request)

  topics = mdb.topics
  topN = await _get_topn(topics, topn=topn)

  contexts = mdb.contexts
  out_dict = {}
  for i, (ngrmm, cnt, conts) in enumerate(topN, 1):
    congr = defaultdict(Counter)

    async for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$lookup': {
        'from': 'topics', 'localField': '_id',
        'foreignField': 'linked_papers.cont_id', 'as': 'cont'}},
      {'$unwind': '$cont'},
      {'$unwind': '$cont.linked_papers'},
      {'$match': {'$expr': {'$eq': ["$_id", "$cont.linked_papers.cont_id"]}}},
      {'$project': {'cont.type': False}}, # 'cont.linked_papers': False,
      # {'$sort': {'frag_num': 1}},
    ]):
      cont = doc['cont']
      ngr = cont['title']
      # if ngr not in exists:
      #   continue
      fnum = doc['frag_num']
      congr[ngr][fnum] += 1

    frags = congr.pop(ngrmm)
    crosstopics = {}
    out_dict[ngrmm] = dict(sum=cnt, frags=frags, crosstopics=crosstopics)


    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crosstopics[co] = dict(frags=cnts, sum=sum(cnts.values()))

  return json_response(out_dict)


async def _get_topn(
  colls, topn:Optional[int], *, preselect:Optional[Sequence]=None,
  sum_expr:Union[int, str]=1
) -> Tuple:
  get_as_tuple = itemgetter('_id', 'count', 'conts')
  if preselect:
    pipeline = list(preselect)
  else:
    pipeline = []
  pipeline += [
    {'$unwind': '$linked_papers'},
    {'$group': {
      '_id': '$title', 'count': {'$sum': sum_expr},
      'conts': {'$addToSet': '$linked_papers.cont_id'}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  topN = tuple([get_as_tuple(doc) async for doc in colls.aggregate(pipeline)])

  return topN


async def _req_top_ngramm(request: web.Request) -> web.StreamResponse:
  """Топ N фраз"""
  app = request.app
  mdb = app['db']

  topn = _get_arg_topn(request)

  nka = _get_arg_int(request, 'nka')
  ltype = _get_arg(request, 'ltype')
  if nka or ltype:
    preselect = [
      {'$match': {f: v for f, v in (('nka', nka), ('type', ltype)) if v}}]
  else:
    preselect = None

  n_gramms = mdb.n_gramms
  topN = await _get_topn(
    n_gramms, topn, preselect=preselect, sum_expr='$linked_papers.cnt')

  out = tuple(dict(title=n, contects=conts) for n, _, conts in topN)
  return json_response(out)


async def _req_top_topics(request: web.Request) -> web.StreamResponse:
  """Топ N топиков"""
  app = request.app
  mdb = app['db']

  topn = _get_arg_topn(request)

  topics = mdb.topics
  topN = await _get_topn(topics, topn=topn)

  out = tuple(dict(title=n, contects=conts) for n, _, conts in topN)
  return json_response(out)


if __name__ == '__main__':
  main()

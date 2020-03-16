# -*- codong: utf-8 -*-
"""
Запросы сделанные в первую итерацию
"""
from collections import Counter, defaultdict
from functools import partial
import logging
from operator import itemgetter

from aiohttp import web
from pymongo.collection import Collection
from server_get_top import (
  _get_topn_cocit_authors, _get_topn_ngramm, _get_topn_topics,
  get_ngramm_filter)

from server_utils import (
  json_response, getreqarg_topn, getreqarg_int, getreqarg, getreqarg_nka,
  getreqarg_ltype)


_logger = logging.getLogger('cirtec')


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


async def _req_frags_pubs(
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


async def _req_frags_cocitauthors(request: web.Request) -> web.StreamResponse:
  """
  А
  Распределение «со-цитируемые авторы» по 5-ти фрагментам
  """
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  contexts = mdb.contexts

  pipeline = [
    {'$match': {'frag_num': {'$gt': 0}, 'cocit_authors': {'$exists': True}}},
    {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
    {'$unwind': '$cocit_authors'},
    {'$group': {
        '_id': '$cocit_authors', 'count': {'$sum': 1},
        'frags': {'$push': '$frag_num'}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn},]
  out_dict = {}
  async for doc in contexts.aggregate(pipeline):
    frags = Counter(doc['frags'])
    out_dict[doc['_id']] = dict(sum=doc['count'], frags=frags)

  return json_response(out_dict)


async def _req_publ_publications_cocitauthors(
  request: web.Request
) -> web.StreamResponse:
  """
  А
  Кросс-распределение «со-цитируемые авторы» по публикациям
  """
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  publications = mdb.publications
  pubs = {
    pdoc['_id']: pdoc['name']
    async for pdoc in publications.find({'name': {'$exists': True}})
  }

  pipeline = [
    #{'$match': {'pub_id': pub_id, 'cocit_authors': {'$exists': True}}},
    {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
    {'$unwind': '$cocit_authors'},
    {'$group': {
      '_id': '$cocit_authors', 'count': {'$sum': 1},
      'conts': {'$addToSet': '$_id'}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}, ]

  out_dict = {}

  contexts = mdb.contexts
  for pub_id, pub_desc in pubs.items():
    cocitauthors = {}
    out_dict[pub_id] = od = dict(descr=pub_desc)
    pipeline_work = [{
      '$match': {'pub_id': pub_id, 'cocit_authors': {'$exists': True}}},
    ] + pipeline

    conts2aut = defaultdict(set)
    async for doc in contexts.aggregate(pipeline_work):
      a = doc['_id']
      conts = doc['conts']
      cocitauthors[a] = tuple(sorted(conts))
      for c in conts:
        conts2aut[c].add(a)

    if conts2aut:
      od.update(
        conts=tuple(sorted(conts2aut.keys())), conts_len=len(conts2aut))
    if cocitauthors:
      # od.update(cocitauthors=cocitauthors)
      occa = {}
      for a, cs in cocitauthors.items():
        occa[a] = ac = defaultdict(set)
        for c in cs:
          for a2 in conts2aut[c]:
            if a2 == a:
              continue
            ac[a2].add(c)
      od.update(
        cocitauthors={
          a: {a2: tuple(sorted(cc)) for a2, cc in v.items()}
          for a, v in occa.items()},
        cocitauthors_len=len(occa))

  return json_response(out_dict)


async def _req_publ_publications_ngramms(
  request: web.Request
) -> web.StreamResponse:
  """Кросс-распределение «фразы из контекстов цитирований» по публикациям"""
  app = request.app
  mdb = app['db']

  publications = mdb.publications
  pubs = {
    pdoc['_id']: pdoc['name']
    async for pdoc in publications.find({'name': {'$exists': True}}).sort('_id')
  }

  topn = getreqarg_topn(request)
  if not topn:
    topn = 10

  nka:int = getreqarg_nka(request)
  ltype:str = getreqarg_ltype(request)

  if nka or ltype:
    postmath = [
      {'$match': {
        f: v for f, v in (('cont.nka', nka), ('cont.type', ltype)) if v}}]
  else:
    postmath = None

  pipeline = [
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_topics': 0,
      'positive_negative': 0, 'bundles': 0},},
    {'$unwind': '$linked_papers_ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'cont'}},
    {'$unwind': '$cont'},
  ]
  if postmath:
    pipeline += postmath

  contexts = mdb.contexts
  n_gramms = mdb.n_gramms

  out_pub_list = []

  for pub_id, pub_desc in pubs.items():
    topN = await _get_topn_ngramm(
      n_gramms, nka, ltype, topn, pub_id=pub_id, title_always_id=True,
      show_type=True)
    exists = frozenset(map(itemgetter(0), topN))

    out_list = []
    oconts = set()

    for i, (ngrmm, ntype, cnt, conts) in enumerate(topN, 1):
      congr = defaultdict(set)
      ngrms = {}

      work_pipeline = [
        {'$match': {'_id': {'$in': conts}, 'pub_id': pub_id}}
      ] + pipeline + [
        {'$match': {'cont.type': ntype}}
      ]
      # _logger.debug('pipeline: %s', work_pipeline)
      async for doc in contexts.aggregate(work_pipeline):
        cont = doc['cont']
        ngr_id = cont['_id']
        ngr = cont['title']
        if ngr_id not in exists:
          continue
        cid = doc['_id']
        oconts.add(cid)
        congr[ngr_id].add(cid)
        ngrms[ngr_id] = dict(type=cont['type'], title=ngr, nka=cont['nka'])

      pubs = congr.pop(ngrmm)
      b_ngrm = ngrms.pop(ngrmm)
      crossgrams = []

      for j, (co, vals) in enumerate(
        sorted(congr.items(), key=lambda kv: (-len(kv[1]), kv[0])), 1
      ):
        co_ = ngrms[co]
        crossgrams.append(
          dict(type=co_['type'], title=co_['title'], conts_len=len(vals)))

      out_list.append(dict(
        type=b_ngrm['type'], title=b_ngrm['title'], nka=b_ngrm['nka'],
        conts=tuple(sorted(pubs)), conts_len=len(pubs),
        crossgrams=crossgrams, crossgrams_len=len(crossgrams)))

    out_pub_list.append(dict(
      pub_id=pub_id, descr=pub_desc, ngrams=out_list, ngrams_len=len(out_list),
      conts=tuple(sorted(oconts)), conts_len=len(oconts)))

  return json_response(out_pub_list)


async def _req_publ_publications_topics(request: web.Request) -> web.StreamResponse:
  """Кросс-распределение «топики контекстов цитирований» по публикациям"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  publications = mdb.publications
  pubs = {
    pdoc['_id']: pdoc['name']
    async for pdoc in publications.find(
      {'name': {'$exists': True}, 'uni_authors': 'Sergey-Sinelnikov-Murylev'})
  }

  topics = mdb.topics
  contexts = mdb.contexts

  out_pubs = []

  for pub_id, pub_desc in pubs.items():
    topN = await _get_topn_topics(topics, topn=topn, pub_id=pub_id)

    out_tops = []
    oconts = set()

    for i, (topic, cnt, conts) in enumerate(topN, 1):
      congr = defaultdict(set)

      pipeline = [
        {'$match': {'_id': {'$in': conts}, 'pub_id': pub_id}},
        {'$project': {
          'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_ngrams': 0,
          'bundles': 0, 'positive_negative': 0}},
        # {'$lookup': {
        #     'from': 'topics', 'localField': '_id',
        #     'foreignField': 'linked_papers.cont_id', 'as': 'cont'}},
        # {'$unwind': '$cont'},
        # {'$unwind': '$cont.linked_papers'},
        # {'$match': {'$expr': {'$eq': ["$_id", "$cont.linked_papers.cont_id"]}}},
        # {'$project': {'cont.type': False}},  # 'cont.linked_papers': False,
        # {'$sort': {'frag_num': 1}},
        {'$unwind': '$linked_papers_topics'},
      ]
      # _logger.debug('pipeline: %s', pipeline)
      async for doc in contexts.aggregate(pipeline):
        ngr = doc['linked_papers_topics']['_id']
        cid = doc['_id']
        oconts.add(cid)
        congr[ngr].add(cid)

      pubs = congr.pop(topic, ())
      crosstopics = []

      for j, (co, vals) in enumerate(
        sorted(congr.items(), key=lambda kv: (-len(kv[1]), kv[0])), 1
      ):
        crosstopics.append(dict(
          topic=co, cnt=len(vals), topics=tuple(sorted(vals))))

      out_tops.append(dict(
        topic=topic, conts=tuple(sorted(pubs)), conts_len=len(pubs),
        crosstopics=crosstopics, crosstopics_len=len(crosstopics)))

    out_pubs.append(dict(
      pub_id=pub_id,
      descr=pub_desc, topics_len=len(out_tops), topics=out_tops,
      conts_len=len(oconts), conts=tuple(sorted(oconts)),
    ))

  return json_response(out_pubs)


async def _req_frags_cocitauthors_cocitauthors(
  request: web.Request
) -> web.StreamResponse:
  """
  А
  Кросс-распределение «5 фрагментов» - «со-цитируемые авторы»
  """
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = [
    {'$match': {
      'cocit_authors': {'$exists': True}, 'frag_num': {'$exists': True},}},
    {'$project': {
      'pub_id': True, 'cocit_authors': True, 'frag_num': True}},
    {'$lookup': {
      'from': 'publications', 'localField': 'pub_id', 'foreignField': '_id',
      'as': 'pub'}},
    {'$unwind': '$pub'},
    {'$match': {'pub.uni_authors': 'Sergey-Sinelnikov-Murylev'}},
    {'$project': {'pub': False}},
    {'$unwind': '$cocit_authors'},
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}},
    {'$project': {
        'pub_id': True, 'cocit_authors': True, 'frag_num': True,
        'cont.cocit_authors': True}},
    {'$unwind': '$cont'},
    {'$unwind': '$cont.cocit_authors'},
    {'$match': {'$expr': {'$ne': ['$cocit_authors', '$cont.cocit_authors']}}},
    {'$group': {
      '_id': {
        'cocitauthor1': {
          '$cond': [{'$gte': ['$cocit_authors', '$cont.cocit_authors'], },
            '$cont.cocit_authors', '$cocit_authors']},
        'cocitauthor2': {
          '$cond': [{'$gte': ['$cocit_authors', '$cont.cocit_authors'], },
            '$cocit_authors', '$cont.cocit_authors']},
        'cont_id': '$_id'},
      'cont': {
        '$first': {
          'pub_id': '$pub_id', 'frag_num': '$frag_num', }}, }},
    {'$sort': {'_id': 1}},
    {'$group': {
      '_id': {
        'cocitauthor1': '$_id.cocitauthor1',
        'cocitauthor2': '$_id.cocitauthor2'},
      'count': {'$sum': 1},
      'conts': {
        '$push': {
          'cont_id': '$_id.cont_id', 'pub_id': '$cont.pub_id',
          'frag_num': '$cont.frag_num', }, }, }},
    {'$sort': {'count': -1, '_id': 1}},
    {'$project': {
      '_id': False,
      'cocitpair': {
        'author1': '$_id.cocitauthor1', 'author2': '$_id.cocitauthor2'},
      'count': '$count', 'conts': '$conts', }},
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = []

  async for doc in curs:
    cocitpair = doc['cocitpair']
    conts = doc['conts']
    pub_ids = tuple(frozenset(map(itemgetter('pub_id'), conts)))
    cont_ids = tuple(map(itemgetter('cont_id'), conts))
    frags = Counter(map(itemgetter('frag_num'), conts))
    out.append(dict(
      cocitpair=tuple(cocitpair.values()),
      cont_cnt=len(cont_ids), pub_cnt=len(pub_ids),
      frags=dict(sorted(frags.items())), pub_ids=pub_ids, cont_ids=cont_ids))

  return json_response(out)


async def _req_publ_cocitauthors_cocitauthors(
  request: web.Request
) -> web.StreamResponse:
  """
  А
  Кросс-распределение «публикации» - «со-цитируемые авторы»
  """
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)
  topn_cocitauthors: int = getreqarg_int(request, 'topn_cocitauthors')

  contexts = mdb.contexts
  topN = await _get_topn_cocit_authors(contexts, topn)

  exists = ()
  if topn_cocitauthors:
    if not topn or topn >= topn_cocitauthors:
      exists = frozenset(map(itemgetter(0), topN[:topn_cocitauthors]))
    else:
      topNa = await _get_topn_cocit_authors(contexts, topn_cocitauthors)
      exists = frozenset(map(itemgetter(0), topNa))

  out_list = []
  for i, (cocitauthor, _) in enumerate(topN, 1):
    cnt = 0
    pubs = set()
    coaut = defaultdict(set)
    async for doc in contexts.find(
      {'cocit_authors': cocitauthor, 'frag_num': {'$gt': 0}},
      projection=['pub_id', 'cocit_authors']
    ).sort('frag_num'):
      # print(i, doc)
      if exists:
        coauthors = frozenset(
          c for c in doc['cocit_authors'] if c != cocitauthor and c in exists)
      else:
        coauthors = frozenset(
          c for c in doc['cocit_authors'] if c != cocitauthor)
      if not coauthors:
        continue
      cnt += 1
      pub_id = doc['pub_id']
      pubs.add(pub_id)
      for ca in coauthors:
        coaut[ca].add(pub_id)

    if not coaut:
      continue

    out_cocitauthors = []
    out_list.append(
      dict(
        title=cocitauthor, cnt_pubs=len(pubs), cnt_cross=len(coaut),
        pubs=tuple(sorted(pubs)), cocitauthors=out_cocitauthors))

    for j, (co, vals) in enumerate(
      sorted(coaut.items(), key=lambda kv: (-len(kv[1]), kv[0])), 1
    ):
      out_cocitauthors.append(
        dict(title=co, cnt_pubs=len(vals), pubs=tuple(sorted(vals))))

  return json_response(out_list)


async def _req_frags_cocitauthors_ngramms(request: web.Request) -> web.StreamResponse:
  """Б Кросс-распределение «со-цитирования» - «фразы из контекстов цитирований»"""
  app = request.app
  mdb = app['db']

  topn:int = getreqarg_topn(request)
  # if not topn:
  #   topn = 10
  topn_gramm:int = getreqarg_int(request, 'topn_gramm')
  if not topn_gramm:
    topn_gramm = 500

  nka:int = getreqarg_nka(request)
  ltype:str = getreqarg_ltype(request)

  if topn_gramm:
    n_gramms = mdb.n_gramms
    top_ngramms = await _get_topn_ngramm(
      n_gramms, nka, ltype, topn_gramm, title_always_id=True)
    exists = frozenset(t for t, _, _ in top_ngramms)
  else:
    exists = ()

  contexts = mdb.contexts
  topN = await _get_topn_cocit_authors(contexts, topn, include_conts=True)

  pipeline = [
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'bundles': 0, 'linked_papers_topics': 0}},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'cont'}},
    {'$unwind': '$cont'},
  ]

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'cont')]

  pipeline += [
    {'$unwind': '$linked_papers_ngrams'},
    {'$match': {'$expr': {'$eq': ['$cont._id', '$linked_papers_ngrams._id']}}},
  ]

  out_dict = {}
  for i, (cocitauthor, cnt, conts) in enumerate(topN, 1):
    frags = Counter()
    congr = defaultdict(Counter)
    titles = {}
    types = {}

    work_pipeline = [
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
    ] +  pipeline

    # _logger.debug('cocitauthor: "%s", cnt: %s, pipeline: %s', cocitauthor, cnt, work_pipeline)
    async for doc in contexts.aggregate(work_pipeline):
      cont = doc['cont']
      ngr_title = cont['title']
      ngr_id = cont['_id']
      if topn_gramm and ngr_id not in exists:
        continue

      fnum = doc['frag_num']
      frags[fnum] += 1
      ngr_cnt = doc['linked_papers_ngrams']['cnt']
      congr[ngr_id][fnum] += ngr_cnt
      titles[ngr_id] = ngr_title
      types[ngr_id] = cont['type']

    crossgrams = []
    out_dict[cocitauthor] = dict(sum=cnt, frags=frags, crossgrams=crossgrams)

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      ngr = dict(
        title=titles[co], type=types[co], frags=cnts, sum=sum(cnts.values()))
      crossgrams.append(ngr)

  return json_response(out_dict)


async def _req_frags_cocitauthors_topics(request: web.Request) -> web.StreamResponse:
  """Кросс-распределение «со-цитирования» - «топики контекстов цитирований»"""
  app = request.app
  mdb = app['db']

  topn:int = getreqarg_topn(request)
  topn_topics:int = getreqarg_int(request, 'topn_topics')
  if topn_topics:
    topics = mdb.topics
    top_topics = await _get_topn_topics(topics, topn=topn)
    exists = frozenset(t for t, _, _ in top_topics)
  else:
    exists = ()

  contexts = mdb.contexts
  topN = await _get_topn_cocit_authors(contexts, topn, include_conts=True)

  pipeline = [
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'bundles': 0, 'linked_papers_ngrams': 0, 'cocit_authors': 0}},
    {'$unwind': '$linked_papers_topics'},
    {'$lookup': {
      'from': 'topics', 'localField': 'linked_papers_topics._id',
      'foreignField': '_id', 'as': 'cont'}},
    {'$unwind': '$cont'},
    {'$match': {'$expr': {'$eq': ['$cont._id', '$linked_papers_topics._id']}}},
  ]

  out_dict = {}
  for i, (cocitauthor, cnt, conts) in enumerate(topN, 1):
    frags = Counter()
    congr = defaultdict(Counter)

    work_pipeline = [
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
    ] + pipeline
    # _logger.debug('cocitauthor: "%s", cnt: %s, pipeline: %s', cocitauthor, cnt, work_pipeline)

    async for doc in contexts.aggregate(work_pipeline):
      cont = doc['cont']
      topic = cont['title']
      if topn_topics and topic not in exists:
        continue

      fnum = doc['frag_num']
      frags[fnum] += 1
      congr[topic][fnum] += 1

    crosstopics = {}
    out_dict[cocitauthor] = dict(sum=cnt, frags=frags, crosstopics=crosstopics)

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crosstopics[co] = dict(frags=cnts, sum=sum(cnts.values()))

  return json_response(out_dict)


async def _req_top_cocitauthors(request: web.Request) -> web.StreamResponse:
  """Топ N со-цитируемых авторов"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  contexts = mdb.contexts
  topN = await _get_topn_cocit_authors(contexts, topn, include_conts=True)
  out = tuple(dict(title=n, cnt=cnt, contects=conts) for n, cnt, conts in topN)

  return json_response(out)


async def _req_top_cocitauthors_pubs(request: web.Request) -> web.StreamResponse:
  """Топ N со-цитируемых авторов по публикациям"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  contexts = mdb.contexts
  topN = await _get_topn_cocit_authors(contexts, topn, include_conts=True)
  out = {
    n: dict(all=cnt, pubs=Counter(c.rsplit('@', 1)[0] for c in conts))
    for n, cnt, conts in topN}

  return json_response(out)


async def _req_frags_ngramm(request: web.Request) -> web.StreamResponse:
  """Распределение «5 фрагментов» - «фразы из контекстов цитирований»"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)
  if not topn:
    topn = 10

  nka:int = getreqarg_nka(request)
  ltype:str = getreqarg_ltype(request)

  pipeline = [
    {'$match': {
      'frag_num': {'$exists': 1}, 'linked_papers_ngrams': {'$exists': 1}}},
    {'$project': {
      '_id': 1, 'frag_num': 1, 'linked_paper': '$linked_papers_ngrams'}},
    {'$unwind': '$linked_paper'},
    {'$group': {
      '_id': {'_id': '$linked_paper._id', 'frag_num': '$frag_num'},
      'count': {'$sum': '$linked_paper.cnt'},}},
    {'$group': {
      '_id': '$_id._id', 'count': {'$sum': '$count'},
      'frags': {'$push': {'frag_num': '$_id.frag_num', 'count': '$count',}},}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$lookup': {
      'from': 'n_gramms', 'localField': '_id', 'foreignField': '_id',
      'as': 'ngramm'}},
    {'$unwind': '$ngramm'},
  ]

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngramm')]

  pipeline += [
    {'$project': {
      'title': '$ngramm.title', 'type': '$ngramm.type', 'nka': '$ngramm.nka',
      'count': '$count', 'frags': '$frags'}}]

  if topn:
    pipeline += [{'$limit': topn}]

  _logger.debug('pipeline: %s', pipeline)
  contexts = mdb.contexts
  out_dict = {}

  async for doc in contexts.aggregate(pipeline):
    title = doc['title']
    cnt = doc['count']
    frags = {n: 0 for n in range(1, 6)}
    frags.update(map(itemgetter('frag_num', 'count'), doc['frags']))
    dtype = doc['type']
    out = dict(sum=cnt, frags=frags)
    if not nka:
      out.update(nka=doc['nka'])
    if ltype:
      out_dict[title] = out
    else:
      out.update(type = dtype, title=title)
      did = doc['_id']
      out_dict[did] = out

  return json_response(out_dict)


async def _req_frags_ngramm_ngramm(request: web.Request) -> web.StreamResponse:
  """Кросс-распределение «5 фрагментов» - «фразы из контекстов цитирований»"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)
  if not topn:
    topn = 10

  nka:int = getreqarg_nka(request)
  ltype:str = getreqarg_ltype(request)

  n_gramms = mdb.n_gramms
  topN = await _get_topn_ngramm(
    n_gramms, nka, ltype, topn, title_always_id=True, show_type=True)
  exists = frozenset(t for t, *_ in topN)

  pipeline = [
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'linked_papers_topics': 0, 'bundles': 0}},
    {'$lookup': {
        'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'cont'}},
    {'$unwind': '$cont'},
  ]

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'cont')]

  pipeline += [
    {'$unwind': '$linked_papers_ngrams'},
    {'$match': {'$expr': {'$eq': ['$linked_papers_ngrams._id', '$cont._id']}}},
  ]

  out_list = []
  contexts = mdb.contexts

  for i, (ngrmm, typ_, cnt, conts) in enumerate(topN, 1):
    congr = defaultdict(Counter)
    titles = {}
    types = {}

    work_pipeline = [
                      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}}
                    ] + pipeline
    # _logger.debug('ngrmm: "%s", cnt: %s, pipeline: %s', ngrmm, cnt, work_pipeline)

    async for doc in contexts.aggregate(work_pipeline):
      cont = doc['cont']
      ngr_id = cont['_id']
      if ngr_id not in exists:
        continue
      fnum = doc['frag_num']
      congr[ngr_id][fnum] += doc['linked_papers_ngrams']['cnt']
      titles[ngr_id] = cont['title']
      # if not ltype:
      types[ngr_id] = cont['type']

    frags = congr.pop(ngrmm)
    crossgrams = []
    otype = ltype if ltype else types[ngrmm]
    # out_list[ngrmm] = dict(sum=cnt, frags=frags, crossgrams=crossgrams)
    out_list.append(
      dict(
        title=titles[ngrmm], type=otype, sum=cnt, cnt_cross=len(congr),
        frags=frags, crossgrams=crossgrams))

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])),
      1
    ):
      crossgrams.append(
        dict(
          title=titles[co], type=types[co], frags=cnts, sum=sum(cnts.values())))

  return json_response(out_list)


async def _req_publ_ngramm_ngramm(request: web.Request) -> web.StreamResponse:
  """Кросс-распределение «публикации» - «фразы из контекстов цитирований»"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)
  if not topn:
    topn = 10

  nka:int = getreqarg_nka(request)
  ltype:str = getreqarg_ltype(request)

  n_gramms = mdb.n_gramms
  topN = await _get_topn_ngramm(
    n_gramms, nka, ltype, topn, title_always_id=True, show_type=True)
  exists = frozenset(t for t, *_ in topN)
  out_list = []
  contexts = mdb.contexts

  pipeline = [
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'bundles': 0, 'linked_papers_topics': 0}},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'cont'}},
    {'$unwind': '$cont'},
  ]
  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'cont')]

  pipeline += [
    {'$unwind': '$linked_papers_ngrams'},
    {'$match': {'$expr': {'$eq': ['$linked_papers_ngrams._id', '$cont._id']}}},
  ]
  for i, (ngrmm, typ_, cnt, conts) in enumerate(topN, 1):
    congr = defaultdict(set)
    titles = {}
    types = {}

    work_pipeline = [
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}}
    ] + pipeline

    # _logger.debug('ngrmm: "%s", cnt: %s, pipeline: %s', ngrmm, cnt, work_pipeline)

    async for doc in contexts.aggregate(work_pipeline):
      cont = doc['cont']
      ngr_id = cont['_id']
      if ngr_id not in exists:
        continue
      pub_id = doc['pub_id']
      congr[ngr_id].add(pub_id)
      titles[ngr_id] = cont['title']
      types[ngr_id] = cont['type']

    pubs = congr.pop(ngrmm, None)
    if not pubs:
      continue

    crossgrams = []
    oltype = ltype if ltype else types[ngrmm]

    out_list.append(
      dict(
        title=titles[ngrmm], type=oltype, pubs=tuple(sorted(pubs)),
        crossgrams=crossgrams, cnt_pubs=len(pubs), cnt_cross=len(congr)))

    enum_sort = enumerate(
      sorted(congr.items(), key=lambda kv: (-len(kv[1]), kv[0])), 1)
    for j, (co, vals) in enum_sort:
      crossgrams.append(
        dict(
          title=titles[co], type=types[co], cnt=len(vals),
          pubs=tuple(sorted(vals))))

  return json_response(out_list)


async def _req_frags_ngramms_cocitauthors(request: web.Request) -> web.StreamResponse:
  """Кросс-распределение «фразы» - «со-цитирования»"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)
  if not topn:
    topn = 10

  contexts = mdb.contexts

  topn_authors:int = getreqarg_int(request, 'topn_cocitauthors')
  if topn_authors:
    topNa = await _get_topn_cocit_authors(
      contexts, topn_authors, include_conts=False)
    exists = frozenset(t for t, _ in topNa)
  else:
    exists = ()

  nka:int = getreqarg_int(request, 'nka')
  ltype:str = getreqarg(request, 'ltype')

  n_gramms = mdb.n_gramms
  top_ngramms = await _get_topn_ngramm(
    n_gramms, nka, ltype, topn, title_always_id=True, show_type=True)

  out_dict = []
  for i, (ngramm, typ_, cnt, conts) in enumerate(top_ngramms, 1):
    frags = Counter()
    congr = defaultdict(Counter)
    cnt = 0

    async for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$unwind': '$cocit_authors'},
    ]):
      ngr = doc['cocit_authors']
      if topn_authors and ngr not in exists:
        continue

      cnt += 1
      fnum = doc['frag_num']
      frags[fnum] += 1
      congr[ngr][fnum] += 1

    if cnt:
      crosscocitaith = {}
      # out_dict[ngramm] = dict(
      #   sum=cnt, frags=frags, cocitaithors=crosscocitaith)
      out_dict.append(dict(
        title=ngramm.split('_', 1)[-1], type=typ_,
        sum=cnt, frags=frags, cocitaithors=crosscocitaith))

      for j, (co, cnts) in enumerate(
        sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
      ):
        crosscocitaith[co] = dict(frags=cnts, sum=sum(cnts.values()))

  return json_response(out_dict)


async def _req_frags_ngramms_topics(request: web.Request) -> web.StreamResponse:
  """Кросс-распределение «фразы» - «топики контекстов цитирований»"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)
  if not topn:
    topn = 10

  topn_topics:int = getreqarg_int(request, 'topn_topics')
  if topn_topics:
    topics = mdb.topics
    topNt = await _get_topn_topics(topics, topn=topn_topics)
    exists = frozenset(t for t, _, _ in topNt)
  else:
    exists = ()

  nka:int = getreqarg_nka(request)
  ltype:str = getreqarg_ltype(request)

  n_gramms = mdb.n_gramms
  top_ngramms = await _get_topn_ngramm(
    n_gramms, nka, ltype, topn, title_always_id=True, show_type=True)

  contexts = mdb.contexts

  out_dict = []
  zerro_frags = {n: 0 for n in range(1, 6)}
  for i, (ngrmm, typ_, cnt, conts) in enumerate(top_ngramms, 1):
    frags = Counter(zerro_frags)
    congr = defaultdict(partial(Counter, zerro_frags))

    pipeline = [
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
      {'$project': {
        'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
        'bundles': 0, 'linked_papers_ngrams': 0}},
      {'$unwind': '$linked_papers_topics'},
    ]
    # _logger.debug('ngrmm: "%s", cnt: %s, pipeline: %s', ngrmm, cnt, pipeline)
    async for doc in contexts.aggregate(pipeline):
      cont = doc['linked_papers_topics']
      topic = cont['_id']
      fnum = doc['frag_num']
      frags[fnum] += 1
      congr[topic][fnum] += 1

    crosstopics = {}
    out_dict.append(dict(
      title=ngrmm.split('_', 1)[-1], type=typ_, sum=cnt, frags=frags,
      crosstopics=crosstopics))

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crosstopics[co] = dict(frags=cnts, sum=sum(cnts.values()))

  return json_response(out_dict)


async def _req_frags_topics(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  topics = mdb.topics
  pipeline = [{
    '$lookup': {
      'from': 'contexts', 'localField': '_id',
      'foreignField': 'linked_papers_topics._id', 'as': 'cont'}},
    {'$unwind': '$cont'},
    {'$project': {
      'title': 1, 'linked_paper': '$cont.linked_papers_topics',
      'cont_id': '$cont._id', 'pub_id': '$cont.pub_id',
      'frag_num': '$cont.frag_num'}},
    {'$unwind': '$linked_paper'},
    {'$match': {
      'frag_num': {'$gt': 0}, '$expr': {'$eq': ['$_id', '$linked_paper._id']}}},
    {'$group': {
      '_id': {'title': '$title', 'frag_num': '$frag_num', },
      'count': {'$sum': 1}, }},
    {'$sort': {'_id': 1, 'count': -1}},
    {'$group': {
      '_id': '$_id.title', 'count': {'$sum': '$count'},
      'frags': {'$push': {'frag_num': '$_id.frag_num', 'count': '$count'}}, }},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  out_dict = {}
  async for doc in topics.aggregate(pipeline):
    cnt = doc['count']
    title = doc['_id']
    frags = {n: 0 for n in range(1, 6)}
    frags.update(map(itemgetter('frag_num', 'count'), doc['frags']))
    out_dict[title] = dict(sum=cnt, frags=frags)

  return json_response(out_dict)


async def _req_frags_topics_topics(request: web.Request) -> web.StreamResponse:
  """Кросс-распределение «5 фрагментов» - «топики контекстов цитирований»"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  topics = mdb.topics
  topN = await _get_topn_topics(topics, topn=topn)

  contexts = mdb.contexts
  out_dict = {}
  for i, (ngrmm, cnt, conts) in enumerate(topN, 1):
    congr = defaultdict(partial(Counter, {n: 0 for n in range(1, 6)}))

    pipeline = [
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
      {'$project': {
        'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_ngrams': 0,
        'positive_negative': 0, 'bundles': 0}},
      {'$unwind': '$linked_papers_topics'},
    ]
    # _logger.debug('ngrmm: "%s":, cnt %s, pipeline: %s', ngrmm, cnt, pipeline)
    async for doc in contexts.aggregate(pipeline):

      cont = doc['linked_papers_topics']
      ngr = cont['_id']

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


async def _req_publ_topics_topics(request: web.Request) -> web.StreamResponse:
  """Кросс-распределение «публикации» - «топики контекстов цитирований»"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  topics = mdb.topics
  topN = await _get_topn_topics(topics, topn=topn)

  pipeline = [
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'bundles': 0, 'linked_papers_ngrams': 0}},
    {'$lookup': {
        'from': 'topics', 'localField': 'linked_papers_topics._id',
      'foreignField': '_id', 'as': 'cont'}},
    {'$unwind': '$cont'},
    {'$unwind': '$linked_papers_topics'},
    {'$match': {'$expr': {'$eq': ['$linked_papers_topics._id', '$cont._id']}}},
  ]

  contexts = mdb.contexts
  out_list = []
  for i, (topic, cnt, conts) in enumerate(topN, 1):
    congr = defaultdict(set)

    work_pipeline = [
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
    ] + pipeline
    # _logger.debug('topic: "%s", cnt: %s, pipeline: %s', topic, cnt, work_pipeline)
    async for doc in contexts.aggregate(work_pipeline):
      cont = doc['cont']
      ngr = cont['title']
      pub_id = doc['pub_id']
      congr[ngr].add(pub_id)

    pubs = congr.pop(topic)
    crosstopics = []
    out_list.append(
      dict(
        topic=topic, cnt=len(pubs), pubs=tuple(sorted(pubs)),
        crosstopics=crosstopics))


    for j, (co, vals) in enumerate(
      sorted(congr.items(), key=lambda kv: (-len(kv[1]), kv[0])), 1
    ):
      crosstopics.append(
        dict(title=co, cnt=len(vals), pubs=tuple(sorted(vals))))

  return json_response(out_list)


async def _req_frags_topics_cocitauthors(
  request: web.Request
) -> web.StreamResponse:
  """Кросс-распределение «топики» - «со-цитирования»"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  contexts = mdb.contexts

  topn_authors:int = getreqarg_int(request, 'topn_cocitauthors')
  if topn_authors:
    topNa = await _get_topn_cocit_authors(
      contexts, topn_authors, include_conts=False)
    exists = frozenset(t for t, _ in topNa)
  else:
    exists = ()

  topics = mdb.topics
  topN = await _get_topn_topics(topics, topn=topn)

  out_dict = {}
  for i, (topic, cnt, conts) in enumerate(topN, 1):
    frags = Counter()
    congr = defaultdict(Counter)

    async for doc in contexts.aggregate([
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
      {'$project': {'prefix': False, 'suffix': False, 'exact': False}},
      {'$unwind': '$cocit_authors'},
    ]):
      ngr = doc['cocit_authors']
      if topn_authors and ngr not in exists:
        continue

      fnum = doc['frag_num']
      frags[fnum] += 1
      congr[ngr][fnum] += 1

    crosscocitaith = {}
    out_dict[topic] = dict(sum=cnt, frags=frags, cocitaithors=crosscocitaith)

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crosscocitaith[co] = dict(frags=cnts, sum=sum(cnts.values()))

  return json_response(out_dict)


async def _req_frags_topics_ngramms(request: web.Request) -> web.StreamResponse:
  """Кросс-распределение «топики» - «фразы»"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  topn_crpssgramm: int = getreqarg_int(request, 'topn_crpssgramm')
  topn_gramm: int = getreqarg_int(request, 'topn_gramm')
  if not topn_gramm:
    topn_gramm = 500

  nka = getreqarg_nka(request)
  ltype = getreqarg_ltype(request)

  if topn_gramm:
    n_gramms = mdb.n_gramms

    top_ngramms = await _get_topn_ngramm(
      n_gramms, nka, ltype, topn_gramm, title_always_id=True)
    exists = frozenset(t for t, _, _ in top_ngramms)
  else:
    exists = ()

  pipeline = [
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'positive_negative': 0,
      'bundles': 0, 'linked_papers_topics': 0}},
    {'$unwind': '$linked_papers_ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]

  pipeline += [
    {'$sort': {'ngrm.count_in_linked_papers': -1, 'ngrm.count_all': -1}},
  ]

  top_topics = await _get_topn_topics(mdb.topics, topn)
  contexts = mdb.contexts
  out_dict = {}
  zerro_frags = {n: 0 for n in range(1, 6)}
  for i, (topic, cnt, conts) in enumerate(top_topics, 1):
    frags = Counter(zerro_frags)
    congr = defaultdict(partial(Counter, zerro_frags))

    work_pipeline = [
      {'$match': {'frag_num': {'$gt': 0}, '_id': {'$in': conts}}},
    ] + pipeline

    # _logger.debug('topic: "%s", cnt: %s, pipeline: %s', topic, cnt, work_pipeline)

    async for doc in contexts.aggregate(work_pipeline):
      cont = doc['ngrm']
      ngr = cont['title']
      if exists and cont['_id'] not in exists:
        continue

      fnum = doc['frag_num']
      frags[fnum] += 1
      congr[ngr][fnum] += 1
      if topn_crpssgramm and len(congr) == topn_crpssgramm:
        break

    crossgrams = {}
    out_dict[topic] = dict(sum=cnt, frags=frags, crossgrams=crossgrams)

    for j, (co, cnts) in enumerate(
      sorted(congr.items(), key=lambda kv: (-sum(kv[1].values()), kv[0])), 1
    ):
      crossgrams[co] = dict(frags=cnts, sum=sum(cnts.values()))

  return json_response(out_dict)


async def _req_top_ngramm(request: web.Request) -> web.StreamResponse:
  """Топ N фраз"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  nka = getreqarg_int(request, 'nka')
  ltype = getreqarg(request, 'ltype')

  n_gramms = mdb.n_gramms
  topN = await _get_topn_ngramm(n_gramms, nka, ltype, topn)

  out = tuple(dict(title=n, contects=conts) for n, _, conts in topN)
  return json_response(out)


async def _req_top_ngramm_pubs(request: web.Request) -> web.StreamResponse:
  """Топ N фраз по публикациям"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  nka = getreqarg_int(request, 'nka')
  ltype = getreqarg(request, 'ltype')
  if nka or ltype:
    pipeline = [get_ngramm_filter(nka, ltype)]
  else:
    pipeline = []

  pipeline += [
    {'$unwind': '$linked_papers'},
    {'$group': {
      '_id': '$title', 'count': {'$sum': '$linked_papers.cnt'}, 'conts': {
        '$addToSet': {
          'cont_id': '$linked_papers.cont_id', 'cnt': '$linked_papers.cnt'}}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  n_gramms = mdb.n_gramms
  get_as_tuple = itemgetter('_id', 'count', 'conts')
  topN = [get_as_tuple(obj) async for obj in n_gramms.aggregate(pipeline)]

  get_pubs = itemgetter('cont_id', 'cnt')
  out = {
    name: dict(
      all=cnt, contects=Counter(
        p for p, n in (
          (c.rsplit('@', 1)[0], n) for c, n in (get_pubs(co) for co in conts))
        for _ in range(n)
      ))
    for name, cnt, conts in topN}
  return json_response(out)


async def _req_top_topics(request: web.Request) -> web.StreamResponse:
  """Топ N топиков"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  topics = mdb.topics
  topN = await _get_topn_topics(topics, topn=topn)

  out = tuple(dict(title=n, contects=conts) for n, _, conts in topN)
  return json_response(out)


async def _req_top_topics_pubs(request: web.Request) -> web.StreamResponse:
  """Топ N топиков"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  topics = mdb.topics
  topN = await _get_topn_topics(topics, topn=topn)

  # out = tuple(dict(title=n, contects=conts) for n, _, conts in topN)
  out = {
    n: dict(all=cnt, pubs=Counter(c.rsplit('@', 1)[0] for c in conts))
    for n, cnt, conts in topN}
  return json_response(out)


async def _reg_cnt_ngramm(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  nka = getreqarg_int(request, 'nka')
  ltype = getreqarg(request, 'ltype')
  if nka or ltype:
    pipeline = [get_ngramm_filter(nka, ltype)]
  else:
    pipeline = []

  pipeline += [{'$sort': {'count_all': -1, 'title': 1, 'type': 1}}]
  if topn:
    pipeline += [{'$limit': topn}]

  out = []
  get_as_tuple = itemgetter('title', 'type', 'linked_papers')
  n_gramms = mdb.n_gramms
  async for doc in n_gramms.aggregate(pipeline):
    title, lt, conts = get_as_tuple(doc)
    res = dict(title=title) if ltype else dict(title=title, type=lt)
    cnt_all = cnt_cont = 0
    pubs = set()
    for cid, cnt in (c.values() for c in conts):
      cnt_cont += 1
      cnt_all += cnt
      pubs.add(cid.rsplit('@', 1)[0])
    res.update(
      count_all=doc['count_all'], count=cnt_all, count_conts=cnt_cont,
      conts_pubs=len(pubs))
    out.append(res)

  return json_response(out)


async def _reg_cnt_pubs_ngramm(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  nka = getreqarg_int(request, 'nka')
  ltype = getreqarg(request, 'ltype')
  if nka or ltype:
    pipeline = [get_ngramm_filter(nka, ltype)]
  else:
    pipeline = []

  n_gramms = mdb.n_gramms
  publications:Collection = mdb.publications
  out = {}
  get_as_tuple = itemgetter('title', 'type', 'linked_papers')

  async for pobj in publications.find({'name': {'$exists': True}}):
    pub_id = pobj['_id']
    pipeline_work = [
      {'$match': {'linked_papers.cont_id': {'$regex': f'^{pub_id}@'}}}
    ] + pipeline
    out_ngrs = []

    cont_starts = pub_id + '@'
    async for obj in n_gramms.aggregate(pipeline_work):
      title, lt, conts = get_as_tuple(obj)
      res = dict(title=title) if ltype else dict(title=title, type=lt)
      res.update(count_all=obj['count_all'])
      cnt_all = cnt_cont = 0
      for cid, cnt in (c.values() for c in conts):
        if cid.startswith(cont_starts):
          cnt_cont += 1
          cnt_all += cnt
      res.update(count=cnt_all, count_conts=cnt_cont,
        # conts=conts
      )
      out_ngrs.append(res)

    out_ngrs = sorted(out_ngrs, key=itemgetter('count'), reverse=True)
    if topn:
      out_ngrs = out_ngrs[:topn]
    out[pub_id] = out_ngrs

  return json_response(out)
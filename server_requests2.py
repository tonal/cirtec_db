# -*- codong: utf-8 -*-
"""
Запросы сделанные в первую итерацию
"""

from collections import Counter
from functools import partial
from itertools import chain, groupby, islice
import logging
from operator import itemgetter

from aiohttp import web
from pymongo import ASCENDING
# from pymongo.collection import Collection
from motor.motor_asyncio import AsyncIOMotorCollection as Collection

from server_get_top import (
  get_ngramm_filter, _get_topn_cocit_refs, _get_refauthors_pipeline)
from server_utils import (
  getreqarg_topn, json_response, getreqarg_probability, getreqarg_nka,
  getreqarg_ltype, getreqarg_id, to_out_typed)


_logger = logging.getLogger('cirtec')


async def _req_top_refbundles(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refbindles_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  async for doc in contexts.aggregate(pipeline):
    doc.pop('pos_neg', None)
    doc.pop('frags', None)
    out.append(doc)

  return json_response(out)


def _get_refbindles_pipeline(topn:int=None):
  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_topics': 0,
      'linked_papers_ngrams': 0}},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},  ##
    {'$group': {
      '_id': '$bundles', 'cits': {'$sum': 1}, 'pubs': {'$addToSet': '$pub_id'},
      'pos_neg': {'$push': '$positive_negative'},
      'frags': {'$push': '$frag_num'}, }},
    {'$project': {
        'cits': 1, 'pubs': {'$size': '$pubs'}, 'pos_neg': 1, 'frags': 1}},
    {'$lookup': {
      'from': 'bundles', 'localField': '_id', 'foreignField': '_id',
      'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$project': {
      'cits': 1, 'pubs': 1, 'pubs_ids': 1,
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title', 'pos_neg': 1, 'frags': 1, }},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}}, # {$count: 'cnt'}
  ]
  if topn:
    pipeline += [{'$limit': topn}]
  return pipeline


async def _req_top_refauthors(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refauthors_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  async for row in contexts.aggregate(pipeline):
    row.pop('pos_neg', None)
    row.pop('frags', None)
    out.append(row)

  return json_response(out)


async def _req_pubs_refauthors(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  publications:Collection = mdb.publications
  contexts:Collection = mdb.contexts

  pipeline = _get_refauthors_pipeline(3)

  out = []
  async for pub in publications.find(
    {'uni_authors': 'Sergey-Sinelnikov-Murylev'},
    projection={'_id': True, 'name': True}, sort=[('_id', ASCENDING)]
  ):
    pid = pub['_id']
    pub_pipeline = [{'$match': {'pub_id': pid}}] + pipeline
    ref_authors = []
    async for row in contexts.aggregate(pub_pipeline):
      row.pop('pos_neg', None)
      row.pop('frags', None)
      ref_authors.append(row)

    pub_out = dict(pub_id=pid, name=pub['name'], ref_authors=ref_authors)
    out.append(pub_out)
  return json_response(out)


async def _req_auth_bund4ngramm_tops(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  contexts:Collection = mdb.contexts
  out = dict()

  a_pipeline = _get_refauthors_pipeline()
  a_pipeline += [{'$match': {'cits': 5}}]

  out_acont = []
  ref_authors = frozenset(
    [o['author'] async for o in contexts.aggregate(a_pipeline)])
  async for cont in contexts.aggregate([
    {'$match': {'exact': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False, 'linked_papers_ngrams': False}},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
      'as': 'bun'}},
    {'$unwind': '$bun'},
    {'$unwind': '$bun.authors'},
    {'$match': {'bun.authors': {'$in': list(ref_authors)}}},
    {'$group': {
      '_id': '$_id', 'cnt': {'$sum': 1},
      'authors': {'$addToSet': '$bun.authors'}}},
    {'$sort': {'cnt': -1, '_id': 1}},
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}},
    {'$project': {
      'cont.prefix': False, 'cont.suffix': False, 'cont.exact': False, }},
    {'$unwind': '$cont'},
  ]):
    icont = cont['cont']
    ngramms = icont.get('linked_papers_ngrams')
    oauth = dict(
      cont_id=cont['_id'], ref_authors=cont['authors'],
      topics=icont.get('linked_papers_topics'))
    if ngramms:
      ongs = sorted(
        (
          dict(ngramm=n, type=t, cnt=c)
          for (t, n), c in (
            (ngr['_id'].split('_'), ngr['cnt']) for ngr in ngramms)
          if t == 'lemmas' and len(n.split()) == 2
        ),
        key=lambda o: (-o['cnt'], o['type'], o['ngramm'])
      )
      oauth.update(ngramms=ongs[:5])
    out_acont.append(oauth)

  out.update(ref_auth_conts=out_acont)

  b_pipeline = _get_refbindles_pipeline()
  b_pipeline += [{'$match': {'cits': 5}}]
  out_bund = []
  ref_bund = frozenset([
    o['_id'] async for o in contexts.aggregate(b_pipeline)])
  async for cont in contexts.aggregate([
    {'$match': {'exact': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False, 'linked_papers_ngrams': False}},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$match': {'bundles': {'$in': list(ref_bund)}}},
    {'$group': {'_id': '$_id', 'cnt': {'$sum': 1}}},
    {'$sort': {'cnt': -1, '_id': 1}},
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}},
    {'$project': {
      'cont.prefix': False, 'cont.suffix': False, 'cont.exact': False, }},
    {'$unwind': '$cont'},
    {'$lookup': {
      'from': 'bundles', 'localField': 'cont.bundles', 'foreignField': '_id',
      'as': 'bund'}},
  ]):
    icont = cont['cont']
    ngramms = icont.get('linked_papers_ngrams')
    bundles = [
      dict(
        bundle=b['_id'], year=b['year'], title=b['title'],
        authors=b.get('authors'))
      for b in cont['bund']]
    oauth = dict(
      cont_id=cont['_id'], bundles=bundles,
      topics=icont.get('linked_papers_topics'))
    if ngramms:
      ongs = sorted(
        (
          dict(ngramm=n, type=t, cnt=c)
          for (t, n), c in (
            (ngr['_id'].split('_'), ngr['cnt']) for ngr in ngramms)
          if t == 'lemmas' and len(n.split()) == 2
        ),
        key=lambda o: (-o['cnt'], o['type'], o['ngramm'])
      )
      oauth.update(ngramms=ongs[:5])
    out_bund.append(oauth)

  out.update(ref_bundles=out_bund)

  return json_response(out)


async def _req_bund4ngramm_tops(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)
  probability = getreqarg_probability(request)

  contexts:Collection = mdb.contexts

  out_bund = []
  pipeline = [
    {'$match': {'exact': {'$exists': True}}}, {
      '$project': {'prefix': False, 'suffix': False, 'exact': False, }},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$group': {
      '_id': '$bundles', 'cits': {'$sum': 1}, 'pubs': {'$addToSet': '$pub_id'},
      'conts': {'$addToSet': {
        'cid': '$_id', 'topics': '$linked_papers_topics',
        'ngrams': '$linked_papers_ngrams'}}}},
    {'$lookup': {
      'from': 'bundles', 'localField': '_id', 'foreignField': '_id',
      'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$project': {
      '_id': False,
      'bundle': '$_id', 'cits': True, 'pubs': {'$size': '$pubs'},
      'pubs_ids': '$pubs', 'conts': True,
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title',}},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  get_probab = itemgetter('probability')
  get_first = itemgetter(0)
  get_second = itemgetter(1)
  get_topic = itemgetter('_id', 'probability')
  def topic_stat(it_tp):
    it_tp = tuple(it_tp)
    probabs = tuple(map(get_second, it_tp))
    return dict(
      count=len(it_tp),)
    # probability_avg=mean(probabs),
    # probability_pstdev=pstdev(probabs))
  get_topics = lambda cont: cont.get('topics') or ()
  get_count = itemgetter('count')
  get_ngrs = lambda cont: cont.get('ngrams') or ()
  get_ngr = itemgetter('_id', 'cnt')
  async for cont in contexts.aggregate(pipeline):
    conts = cont.pop('conts')

    cont_ids = map(itemgetter('cid'), conts)

    topics = chain.from_iterable(map(get_topics, conts))
    # удалять топики < 0.5
    topics = ((t, p) for t, p in map(get_topic, topics) if p >= probability)
    topics = (
      dict(topic=t, **topic_stat(it_tp))
      for t, it_tp in groupby(sorted(topics, key=get_first), key=get_first))
    topics = sorted(topics, key=get_count, reverse=True)

    ngrams = chain.from_iterable(map(get_ngrs, conts))
    # только 2-grams и lemmas
    ngrams = (
      (n.split('_', 1)[-1].split(), c)
      for n, c in map(get_ngr, ngrams) if n.startswith('lemmas_'))
    ngrams = ((' '.join(n), c) for n, c in ngrams if len(n) == 2)
    ngrams = (
      dict(ngramm=n, count=sum(map(get_second, it_nc)))
      for n, it_nc in groupby(sorted(ngrams, key=get_first), key=get_first))
    ngrams = sorted(ngrams, key=get_count, reverse=True)
    ngrams = islice(ngrams, 10)
    cont.update(
      cont_ids=tuple(cont_ids),
      topics=tuple(topics),
      ngrams=tuple(ngrams))
    out_bund.append(cont)
  out = out_bund

  return json_response(out)


async def _ref_auth4ngramm_tops(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)
  probability = getreqarg_probability(request)

  contexts:Collection = mdb.contexts

  out_bund = []
  pipeline = [
    {'$match': {'exact': {'$exists': True}}}, {
    '$project': {'prefix': False, 'suffix': False, 'exact': False, }},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
      'as': 'bundle'}},
    {'$unwind': '$bundle'},
    {'$unwind': '$bundle.authors'},
    {'$group': {
      '_id': '$bundle.authors', 'pubs': {'$addToSet': '$pub_id'},
      'conts': {'$addToSet': {
        'cid': '$_id', 'topics': '$linked_papers_topics',
        'ngrams': '$linked_papers_ngrams'}}}},
    {'$project': {
      '_id': False,
      'aurhor': '$_id', 'cits': {'$size': '$conts'}, 'pubs': {'$size': '$pubs'},
      'pubs_ids': '$pubs', 'conts': '$conts',
      'total_cits': '$bundle.total_cits', 'total_pubs': '$bundle.total_pubs',
      'year': '$bundle.year', 'authors': '$bundle.authors',
      'title': '$bundle.title',}},
    {'$sort': {'cits': -1, 'pubs': -1, 'title': 1}},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  get_topics = lambda c: c.get('topics', ())
  get_topic = itemgetter('_id', 'probability')
  get_first = itemgetter(0)
  get_second = itemgetter(1)
  def topic_stat(it_tp):
    it_tp = tuple(it_tp)
    probabs = tuple(map(get_second, it_tp))
    return dict(
      count=len(it_tp),)
      # probability_avg=mean(probabs),
      # probability_pstdev=pstdev(probabs))
  get_count = itemgetter('count')
  get_ngr = itemgetter('_id', 'cnt')
  async for cont in contexts.aggregate(pipeline):
    conts = cont.pop('conts')

    cont_ids = map(itemgetter('cid'), conts)

    topics = chain.from_iterable(map(get_topics, conts))
    # удалять топики < 0.5
    topics = ((t, p) for t, p in map(get_topic, topics) if p >= probability)
    topics = (
      dict(topic=t, **topic_stat(it_tp))
      for t, it_tp in groupby(sorted(topics, key=get_first), key=get_first))
    topics = sorted(topics, key=get_count, reverse=True)

    get_ngrs = lambda cont: cont.get('ngrams') or ()
    ngrams = chain.from_iterable(map(get_ngrs, conts))
    # только 2-grams и lemmas
    ngrams = (
      (n.split('_', 1)[-1].split(), c)
      for n, c in map(get_ngr, ngrams) if n.startswith('lemmas_'))
    ngrams = ((' '.join(n), c) for n, c in ngrams if len(n) == 2)
    ngrams = (
      dict(ngramm=n, count=sum(map(get_second, it_nc)))
      for n, it_nc in groupby(sorted(ngrams, key=get_first), key=get_first))
    ngrams = sorted(ngrams, key=get_count, reverse=True)
    ngrams = islice(ngrams, 10)
    cont.update(
      cont_ids=tuple(cont_ids),
      topics=tuple(topics),
      ngrams=tuple(ngrams))
    out_bund.append(cont)
  out = out_bund

  return json_response(out)


async def _req_pos_neg_pubs(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  contexts: Collection = mdb.contexts
  out = []
  async for doc in contexts.aggregate([
    {'$match': {'positive_negative': {'$exists': True}}, },
    {'$group': {
      '_id': {'pid': '$pub_id', 'pos_neg': '$positive_negative.val'},
      'cnt': {'$sum': 1}}},
    {'$group': {
      '_id': '$_id.pid',
      'pos_neg': {'$push': {'val': '$_id.pos_neg', 'cnt': '$cnt'}}}},
    {'$sort': {'_id': 1}},
    {'$lookup': {
      'from': 'publications', 'localField': '_id', 'foreignField': '_id',
      'as': 'pub'}},
    {'$unwind': '$pub'},
    {'$project': {'pos_neg': True, 'pub.name': True}}
  ]):
    pid:str = doc['_id']
    name:str = doc['pub']['name']
    classif = doc['pos_neg']
    neutral = sum(v['cnt'] for v in classif if v['val'] == 0)
    positive = sum(v['cnt'] for v in classif if v['val'] > 0)
    negative = sum(v['cnt'] for v in classif if v['val'] < 0)
    out.append(
      dict(
        pub=pid, name=name, neutral=int(neutral), positive=int(positive),
        negative=int(negative)))

  return json_response(out)


async def _req_pos_neg_refbundles(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refbindles_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  async for doc in contexts.aggregate(pipeline):
    doc.pop('frags', None)
    classify = doc.pop('pos_neg', None)
    if classify:
      neutral = sum(1 for v in classify if v['val'] == 0)
      positive = sum(1 for v in classify if v['val'] > 0)
      negative = sum(1 for v in classify if v['val'] < 0)
      doc.update(
        class_pos_neg=dict(
          neutral=neutral, positive=positive, negative=negative))
    out.append(doc)

  return json_response(out)


async def _req_pos_neg_refauthors(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refauthors_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  async for row in contexts.aggregate(pipeline):
    row.pop('frags', None)
    classify = row.pop('pos_neg', None)
    if classify:
      neutral = sum(1 for v in classify if v['val'] == 0)
      positive = sum(1 for v in classify if v['val'] > 0)
      negative = sum(1 for v in classify if v['val'] < 0)
      row.update(
        class_pos_neg=dict(
          neutral=neutral, positive=positive, negative=negative))
    out.append(row)

  return json_response(out)


async def _req_frags_refbundles(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refbindles_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  async for doc in contexts.aggregate(pipeline):
    doc.pop('pos_neg', None)
    frags = Counter(doc.pop('frags', ()))
    doc.update(frags=frags)
    out.append(doc)

  return json_response(out)


async def _req_frags_refauthors(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refauthors_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  async for row in contexts.aggregate(pipeline):
    row.pop('pos_neg', None)
    frags = Counter(row.pop('frags', ()))
    row.update(frags=frags)
    out.append(row)

  return json_response(out)


async def _req_by_frags_refauthors(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = _get_refauthors_pipeline(topn)

  contexts:Collection = mdb.contexts
  out = []
  for fnum in range(1, 6):
    out_frag = []
    work_pipe = [
      {'$match': {'frag_num': fnum}}
    ] + pipeline
    async for row in contexts.aggregate(work_pipe):
      row.pop('pos_neg', None)
      row.pop('frags', None)
      out_frag.append(row)

    out.append(dict(frag_num=fnum, refauthors=out_frag))

  return json_response(out)


async def _req_top_ngramm_pubs(request: web.Request) -> web.StreamResponse:
  """Топ N фраз по публикациям"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = [
    {'$match': {'linked_papers_ngrams._id': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False}},
    {'$unwind': '$linked_papers_ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]

  nka = getreqarg_nka(request)
  ltype = getreqarg_ltype(request)

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]

  if ltype:
    gident = '$ngrm.title'
  else:
    gident = {'title': '$ngrm.title', 'type': '$ngrm.type'}

  pipeline += [
  {'$group': {
      '_id': gident, 'count': {'$sum': '$linked_papers_ngrams.cnt'},
      'conts': {
        '$push': {
          'pub_id': '$pub_id', 'cnt': '$linked_papers_ngrams.cnt'}}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]

  if topn:
    pipeline += [{'$limit': topn}]

  # _logger.debug('pipeline: %s', pipeline)

  contexts = mdb.contexts
  get_as_tuple = itemgetter('_id', 'count', 'conts')
  topN = [get_as_tuple(obj) async for obj in contexts.aggregate(pipeline)]

  get_pubs = itemgetter('pub_id', 'cnt')
  if ltype:
    out = {
      name: dict(
        all=cnt, contects=Counter(
          p for p, n in (get_pubs(co) for co in conts)
          for _ in range(n)
        ))
      for name, cnt, conts in topN}
  else:
    out = [
      (name, dict(
        all=cnt, contects=Counter(
          p for p, n in (get_pubs(co) for co in conts)
          for _ in range(n)
        )))
      for name, cnt, conts in topN]
  return json_response(out)


async def _req_top_ngramm(request: web.Request) -> web.StreamResponse:
  """Топ N фраз по публикациям"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = [
    {'$match': {'linked_papers_ngrams._id': {'$exists': True}}},
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_topics': False}},
    {'$unwind': '$linked_papers_ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
  ]

  nka = getreqarg_nka(request)
  ltype = getreqarg_ltype(request)

  if nka or ltype:
    pipeline += [get_ngramm_filter(nka, ltype, 'ngrm')]

  if ltype:
    gident = '$ngrm.title'
  else:
    gident = {'title': '$ngrm.title', 'type': '$ngrm.type'}

  pipeline += [
  {'$group': {
      '_id': gident, 'count': {'$sum': '$linked_papers_ngrams.cnt'},
      'count_cont': {'$sum': 1},
      'conts': {
        '$push': {
          'cont_id': '$_id', 'cnt': '$linked_papers_ngrams.cnt'}}}},
    {'$sort': {'count': -1, '_id': 1}},
  ]

  if topn:
    pipeline += [{'$limit': topn}]

  contexts = mdb.contexts
  get_as_tuple = itemgetter('_id', 'count', 'count_cont', 'conts')
  # _logger.info('pipeline: %s', pipeline)
  topN = [get_as_tuple(obj) async for obj in contexts.aggregate(pipeline)]

  get_pubs = itemgetter('cont_id', 'cnt')
  key_sort = lambda kv: (-kv[-1], kv[0])
  if ltype:
    get_name = lambda n: n
    get_ltype = lambda _: ltype
  else:
    get_name = itemgetter('title') # lambda name: name['title']
    get_ltype = itemgetter('type') # lambda name: name['type']

  out = [
    dict(
      title=get_name(name), type=get_ltype(name),
      all=cnt, count_cont=count_cont,
      contects=dict(
        sorted(
          Counter(
            p for p, n in (get_pubs(co) for co in conts)
            for _ in range(n)
          ).most_common(),
          key=key_sort)))
    for name, cnt, count_cont, conts in topN]

  return json_response(out)


async def _req_db_publication(request: web.Request) -> web.StreamResponse:
  """Публикации"""
  app = request.app
  mdb = app['db']

  pub_id = getreqarg_id(request)
  if not pub_id:
    return json_response({})

  publications:Collection = mdb.publications
  doc = await publications.find_one({'_id': pub_id})
  if not doc:
    return json_response({})
  return json_response(doc)


async def _req_publications(request: web.Request) -> web.StreamResponse:
  """Публикации"""
  app = request.app
  mdb = app['db']

  pub_id = getreqarg_id(request)

  query = (
    dict(_id=pub_id) if pub_id
    else dict(uni_authors='Sergey-Sinelnikov-Murylev'))

  to_out = partial(to_out_typed, type='publication')

  publications = mdb.publications
  out = [
    to_out(**doc) async for doc in
      publications.find(query).sort([('year', ASCENDING), ('_id', ASCENDING)])]
  return json_response(out)


async def _req_db_context(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  cont_id = getreqarg_id(request)
  if not cont_id:
    return json_response({})

  contexts:Collection = mdb.contexts
  doc = await contexts.find_one({'_id': cont_id})
  if not doc:
    return json_response({})
  return json_response(doc)


async def _req_contexts(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  cont_id = getreqarg_id(request)
  if not cont_id:
    return json_response([])

  to_out = partial(to_out_typed, type='context')

  contexts = mdb.contexts
  pipeline = [
    {'$match': {'_id': cont_id}},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'linked_papers_ngrams'}},
  ]
  out = [to_out(**doc) async for doc in contexts.aggregate(pipeline)]
  return json_response(out)


async def _req_db_bundle(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  bund_id = getreqarg_id(request)
  if not bund_id:
    return json_response({})

  bundles:Collection = mdb.bundles
  doc:dict = await bundles.find_one(dict(_id=bund_id))
  if not doc:
    return json_response({})
  return json_response(doc)


async def _req_bundles(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  bund_id = getreqarg_id(request)
  if not bund_id:
    return json_response([])

  to_out = partial(to_out_typed, type='bundle')

  bundles:Collection = mdb.bundles
  pipeline = [
    {'$match': {'_id': bund_id}},
    {'$lookup': {
      'from': 'publications', 'localField': '_id',
      'foreignField': 'refs.bundle', 'as': 'pub'}},
    {'$unwind': '$pub'},
    {'$unwind': '$pub.refs'},
    {'$match': {'$expr': {'$eq': ['$_id', '$pub.refs.bundle']}}},
    {'$project': {'pub.refs.bundle': 0}},
    {'$sort': {'pub.year': 1, 'pub._id': 1, 'pub.refs.num': 1}},
    {'$group': {
      '_id': '$_id', 'authors': {'$first': '$authors'},
      'title': {'$first': '$title'}, 'year': {'$first': '$year'},
      'total_cits': {'$first': '$total_cits'},
      'total_pubs': {'$first': '$total_pubs'},
      'refs': {'$push': {
        'pubid': '$pub._id', 'num': '$pub.refs.num',
        'title': '$pub.refs.title', 'authors': '$pub.refs.authors',
        'year': '$pub.refs.year'}}, }},
  ]
  async for doc in bundles.aggregate(pipeline):
    out = to_out_typed(type='bundle', **doc)
    return json_response(out)
  return json_response([])


async def _req_db_topic(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topic_id = getreqarg_id(request)
  if not topic_id:
    return json_response({})

  topics:Collection = mdb.topics
  doc:dict = await topics.find_one(dict(_id=topic_id))
  if not doc:
    return json_response({})
  return json_response(doc)


async def _req_db_ngramm(request: web.Request) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  oid = getreqarg_id(request)
  if not oid:
    return json_response({})

  n_gramms:Collection = mdb.n_gramms
  doc:dict = await n_gramms.find_one(dict(_id=oid))
  if not doc:
    return json_response({})
  return json_response(doc)


async def _req_pub_cont_bund(request: web.Request) -> web.StreamResponse:
  _id = getreqarg_id(request)

  if not _id:
    return json_response([])

  if '@' in _id:
    return await _req_contexts(request)

  if ':' in _id:
    return await _req_publications(request)

  return await _req_bundles(request)


async def _req_top_topics(request: web.Request) -> web.StreamResponse:
  """Топ N топиков"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  probability = getreqarg_probability(request)

  pipeline = [
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_ngrams': False}},
    {'$unwind': '$linked_papers_topics'},
    {'$match': {'linked_papers_topics.probability': {'$gte': probability}}},
    {'$group': {
      '_id': '$linked_papers_topics._id', 'count': {'$sum': 1},
      'probability_avg': {'$avg': '$linked_papers_topics.probability'},
      'probability_stddev': {'$stdDevPop': '$linked_papers_topics.probability'},
      'conts': {'$addToSet': '$_id'}, }},
    {'$sort': {'count': -1, '_id': 1}}
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  to_out = lambda _id, count, probability_avg, probability_stddev, conts: dict(
    topic=_id, count=count, probability_avg=round(probability_avg, 2),
    probability_stddev=round(probability_stddev, 2),
    contects=conts)

  out = [to_out(**doc) async for doc in curs]
  return json_response(out)


async def _req_top_topics_pubs(request: web.Request) -> web.StreamResponse:
  """Топ N топиков"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  probability = getreqarg_probability(request)

  pipeline = [
    {'$project': {
      'prefix': False, 'suffix': False, 'exact': False,
      'linked_papers_ngrams': False}},
    {'$unwind': '$linked_papers_topics'},
    {'$match': {'linked_papers_topics.probability': {'$gte': probability}}},
    {'$group': {
      '_id': '$linked_papers_topics._id', 'count': {'$sum': 1},
      'probability_avg': {'$avg': '$linked_papers_topics.probability'},
      'probability_stddev': {'$stdDevPop': '$linked_papers_topics.probability'},
      'pubs': {'$addToSet': '$pub_id'}, }},
    {'$project': {
      'count_pubs': {'$size': '$pubs'}, 'count_conts': '$count',
      'probability_avg': {'$round': ['$probability_avg', 2]},
      'probability_stddev': {'$round': ['$probability_stddev', 2]}, 'pubs': 1}},
    {'$sort': {'count_pubs': -1, 'count_conts': -1, '_id': 1}}
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  to_out = lambda _id, pubs, **kwds: dict(topic=_id, **kwds, publications=pubs)

  out = [to_out(**doc) async for doc in curs]
  return json_response(out)


async def _req_pos_neg_contexts(request: web.Request) -> web.StreamResponse:
  """2.1.7. Общее распределение классов тональности для контекстов из всех
    публикаций заданного автора
    - для каждого класса тональности показать общее количество контекстов,
    которые к ним относятся
  """
  app = request.app
  mdb = app['db']

  pipeline = [
    {'$match': {'positive_negative': {'$exists': True}}},
    {'$project': {'pub_id': True, 'positive_negative': True}},
    {'$lookup': {
      'from': 'publications', 'localField': 'pub_id', 'foreignField': '_id',
      'as': 'pub'}},
    {'$unwind': '$pub'},
    {'$match': {'pub.uni_authors': 'Sergey-Sinelnikov-Murylev'}},
    {'$group': {
      '_id': {
        '$arrayElemAt': [
          ['neutral', 'positive', 'positive', 'negative', 'negative'],
          '$positive_negative.val']},
      'pub_ids': {'$addToSet': '$pub_id'},
      'cont_ids': {'$addToSet': '$_id'}}},
    {'$project': {
      '_id': False, 'class_pos_neg': '$_id',
      'cont_cnt': {'$size': '$cont_ids'}, 'pub_cnt': {'$size': '$pub_ids'},
      'pub_ids': '$pub_ids', 'cont_ids': '$cont_ids'}},
    {'$sort': {'class_pos_neg': -1}},
  ]

  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]

  return json_response(out)


async def _req_pos_neg_ngramms(request: web.Request) -> web.StreamResponse:
  """2.3.4а. Тональность для фраз
    - для каждого класса тональности показать топ фраз с количеством повторов каждой
  """
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request, default=10)

  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': True},
      'linked_papers_ngrams': {'$exists': True},}},
    {'$project': {
      'pub_id': True, 'positive_negative': True, 'linked_papers_ngrams': True}},
    {'$lookup': {
        'from': 'publications', 'localField': 'pub_id', 'foreignField': '_id',
        'as': 'pub'}},
    {'$unwind': '$pub'},
    {'$match': {'pub.uni_authors': 'Sergey-Sinelnikov-Murylev'}},
    {'$project': {'pub': False}},
    {'$unwind': '$linked_papers_ngrams'},
    {'$lookup': {
      'from': 'n_gramms', 'localField': 'linked_papers_ngrams._id',
      'foreignField': '_id', 'as': 'ngrm'}},
    {'$unwind': '$ngrm'},
    {'$match': {'ngrm.type': 'lemmas', 'ngrm.nka': 2}},
    {'$group': {
      '_id': {
        'pos_neg': {
          '$arrayElemAt': [
            ['neutral', 'positive', 'positive', 'negative', 'negative'],
            '$positive_negative.val']},
        'title': '$ngrm.title'},
      'ngrm_cnt': {'$sum': '$linked_papers_ngrams.cnt'}}},
    {'$sort': {'ngrm_cnt': -1, '_id.title': 1}},
    {'$group': {
      '_id': '$_id.pos_neg',
      'ngramms': {'$push': {'title': '$_id.title', 'count': '$ngrm_cnt'}},}},
    {'$project': {
      '_id': False, 'class_pos_neg': '$_id',
      'ngramms': {'$slice': ['$ngramms', topn]},}},
    {'$sort': {'class_pos_neg': -1}},
  ]

  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]

  return json_response(out)


async def _req_pos_neg_topics(request: web.Request) -> web.StreamResponse:
  """2.3.4б. Тональность для топиков
    - для каждого класса тональности показать топ топиков с количеством
    повторов каждого
  """
  app = request.app
  mdb = app['db']

  probability = getreqarg_probability(request)

  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': True},
      'linked_papers_topics': {'$exists': True}}},
    {'$project': {
      'pub_id': True, 'positive_negative': True, 'linked_papers_topics': True}},
    {'$lookup': {
        'from': 'publications', 'localField': 'pub_id', 'foreignField': '_id',
        'as': 'pub'}},
    {'$unwind': '$pub'},
    {'$match': {'pub.uni_authors': 'Sergey-Sinelnikov-Murylev'}},
    {'$project': {'pub': False}},
    {'$unwind': '$linked_papers_topics'},
    {'$match': {'linked_papers_topics.probability': {'$gte': probability}}},
    {'$group': {
      '_id': {
        'pos_neg': {
          '$arrayElemAt': [
            ['neutral', 'positive', 'positive', 'negative', 'negative'],
            '$positive_negative.val']},
        'topic': '$linked_papers_topics._id'},
      'topic_cnt': {'$sum': 1}}},
    {'$sort': {'topic_cnt': -1}},
    {'$group': {
      '_id': '$_id.pos_neg',
      'topics': {'$push': {'topic': '$_id.topic', 'count': '$topic_cnt'}},}},
    {'$project': {
      '_id': False, 'class_pos_neg': '$_id',
      'topic_cnt': {'$size': '$topics'}, 'topics': '$topics'}},
    {'$sort': {'class_pos_neg': -1}},
  ]

  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]

  return json_response(out)


async def _req_pos_neg_cocitauthors(request: web.Request) -> web.StreamResponse:
  """2.3.5. Тональность для со-цитируемых авторов
    - для каждого класса тональности привести топ со-цитируемых источников/авторов
  """
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request, default=10)

  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': True},
      'cocit_authors': {'$exists': True}}},
    {'$project': {
      'pub_id': True, 'positive_negative': True, 'cocit_authors': True}},
    {'$lookup': {
        'from': 'publications', 'localField': 'pub_id', 'foreignField': '_id',
        'as': 'pub'}},
    {'$unwind': '$pub'},
    {'$match': {'pub.uni_authors': 'Sergey-Sinelnikov-Murylev'}},
    {'$project': {'pub': False}},
    {'$unwind': '$cocit_authors'},
    {'$group': {
      '_id': {
        'pos_neg': {
          '$arrayElemAt': [
            ['neutral', 'positive', 'positive', 'negative', 'negative'],
            '$positive_negative.val']},
        'title': '$cocit_authors'},
      'coauthor_cnt': {'$sum': 1}}},
    {'$sort': {'coauthor_cnt': -1, '_id.title': 1}},
    {'$group': {
      '_id': '$_id.pos_neg',
      'cocitauthor': {
        '$push': {'author': '$_id.title', 'count': '$coauthor_cnt'}},}},
    {'$project': {
      '_id': False, 'class_pos_neg': '$_id',
      'cocitauthors': {'$slice': ['$cocitauthor', topn]} }},
    {'$sort': {'class_pos_neg': -1}},
  ]

  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]

  return json_response(out)


async def _req_frags_pos_neg_contexts(request: web.Request) -> web.StreamResponse:
  """2.3.6. Распределение тональности контекстов по 5-ти фрагментам
    - для каждого класса тональности показать распределение
    соответствующих контектсов по 5-ти фрагментам
  """
  app = request.app
  mdb = app['db']

  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': True}, 'frag_num': {'$exists': True}}},
    {'$project': {'pub_id': True, 'positive_negative': True, 'frag_num': True}},
    {'$lookup': {
      'from': 'publications', 'localField': 'pub_id', 'foreignField': '_id',
      'as': 'pub'}},
    {'$unwind': '$pub'},
    {'$match': {'pub.uni_authors': 'Sergey-Sinelnikov-Murylev'}},
    {'$project': {'pub': False}},
    {'$group': {
      '_id': {
        'class_pos_neg': {
        '$arrayElemAt': [
          ['neutral', 'positive', 'positive', 'negative', 'negative'],
          '$positive_negative.val']},
        'frag_num': '$frag_num'},
      'pub_ids': {'$addToSet': '$pub_id'},
      'cont_ids': {'$addToSet': '$_id'}}},
    {'$sort': {'_id.class_pos_neg': -1}},
    {'$group': {
      '_id': '$_id.frag_num',
      'classes': {'$push': {
          'pos_neg': '$_id.class_pos_neg',
          'pub_ids': '$pub_ids', 'cont_ids': '$cont_ids'}}}},
    {'$project': {
      '_id': False, 'frag_num': '$_id', 'classes': '$classes'}},
    {'$sort': {'frag_num': 1}},
  ]

  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  out = [doc async for doc in curs]

  return json_response(out)


async def _req_frags_pos_neg_cocitauthors2(
  request: web.Request
) -> web.StreamResponse:
  """2.4.2. Со-цитируемые авторы, распределение тональности их
    со-цитирований и распределение по 5-ти фрагментам
    - для каждой пары со-цитируемых авторов показать распределение классов
    тональности контекстов, в которых эта пара со-цитируется и
    распределение этих контекстов по 5-ти фрагментам
  """
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)


  pipeline = [
    {'$match': {
      'positive_negative': {'$exists': True},
      'cocit_authors': {'$exists': True}}},
    {'$project': {
      'pub_id': True, 'positive_negative': True, 'cocit_authors': True,
      'frag_num': True}},
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
      'pub_id': True, 'positive_negative': True, 'cocit_authors': True,
      'frag_num': True, 'cont.cocit_authors': True}},
    {'$unwind': '$cont'},
    {'$unwind': '$cont.cocit_authors'},
    {'$match': {'$expr': {'$ne': ['$cocit_authors', '$cont.cocit_authors']}}},
    {'$group': {
      '_id': {
        'cocitauthor1': {'$cond': [
          {'$gte': ['$cocit_authors', '$cont.cocit_authors'],},
          '$cont.cocit_authors', '$cocit_authors']},
        'cocitauthor2': {'$cond': [
          {'$gte': ['$cocit_authors', '$cont.cocit_authors'],},
          '$cocit_authors', '$cont.cocit_authors']},
        'cont_id': '$_id'},
      'cont': {'$first': {
        'pub_id': '$pub_id', 'frag_num': '$frag_num',
        'positive_negative': '$positive_negative'}},}},
    {'$sort': {'_id': 1}},
    {'$group': {
      '_id': {
        'cocitauthor1': '$_id.cocitauthor1',
        'cocitauthor2': '$_id.cocitauthor2'},
      'count': {'$sum': 1},
      'conts': {'$push': {
        'cont_id': '$_id.cont_id', 'pub_id': '$cont.pub_id',
        'frag_num': '$cont.frag_num',
        'positive_negative': '$cont.positive_negative',},},}},
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
  # out = [doc async for doc in curs]
  out = []
  async for doc in curs:
    cocitpair = doc['cocitpair']
    conts = doc['conts']
    pub_ids = tuple(frozenset(map(itemgetter('pub_id'), conts)))
    cont_ids = tuple(map(itemgetter('cont_id'), conts))
    frags = Counter(map(itemgetter('frag_num'), conts))
    classif = tuple(map(itemgetter('positive_negative'), conts))
    neutral = sum(1 for v in classif  if v['val'] == 0)
    positive = sum(1 for v in classif if v['val'] > 0)
    negative = sum(1 for v in classif if v['val'] < 0)
    out.append(dict(
      cocitpair=tuple(cocitpair.values()),
      cont_cnt=len(cont_ids), pub_cnt=len(pub_ids),
      frags=dict(sorted(frags.items())), neutral=neutral, positive=positive,
      negative=negative, pub_ids=pub_ids, cont_ids=cont_ids))

  return json_response(out)


async def _req_top_cocitrefs(request: web.Request) -> web.StreamResponse:
  """Топ N со-цитируемых авторов"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  def repack(_id, count, conts, bundles):
    authors = bundles.get('authors')
    title = bundles['title']
    year = bundles.get('year', '?')
    descr = f'{" ".join(authors) if authors else "?"} ({year}) {title}'
    return dict(bid=_id, descr=descr, contects=conts)

  contexts = mdb.contexts
  outs = [
    repack(**doc) async for doc in _get_topn_cocit_refs(
      contexts, topn, include_conts=True, include_descr=True)]

  out = tuple(outs)

  return json_response(out)


async def _req_top_cocitrefs2(
  request: web.Request
) -> web.StreamResponse:
  """Со-цитируемые рефереры"""
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)


  pipeline = [
    {'$match': {'cocit_refs': {'$exists': 1}, 'frag_num': {'$exists': 1}}},
    {'$project': {'pub_id': 1, 'cocit_refs': 1, 'frag_num': 1}},
    {'$lookup': {
      'from': 'publications', 'localField': 'pub_id', 'foreignField': '_id',
      'as': 'pub'}},
    {'$unwind': '$pub'},
    {'$match': {'pub.uni_authors': 'Sergey-Sinelnikov-Murylev'}},
    {'$project': {'pub': 0}},
    {'$unwind': '$cocit_refs'},
    {'$lookup': {
      'from': 'contexts', 'localField': '_id', 'foreignField': '_id',
      'as': 'cont'}},
    {'$project': {
      'pub_id': 1, 'cocit_refs': 1, 'frag_num': 1, 'cont.cocit_refs': 1}},
    {'$unwind': '$cont'},
    {'$unwind': '$cont.cocit_refs'},
    {'$match': {'$expr': {'$ne': ['$cocit_refs', '$cont.cocit_refs']}}},
    {'$group': {
      '_id': {
        'cocitref1': {
          '$cond': [{'$gte': ['$cocit_refs', '$cont.cocit_refs'], },
            '$cont.cocit_refs', '$cocit_refs']}, 'cocitref2': {
          '$cond': [{'$gte': ['$cocit_refs', '$cont.cocit_refs'], },
            '$cocit_refs', '$cont.cocit_refs']}, 'cont_id': '$_id'},
      'cont': {
        '$first': {
          'pub_id': '$pub_id', 'frag_num': '$frag_num', }}, }},
    {'$sort': {'_id': 1}},
    {'$group': {
        '_id': {
          'cocitref1': '$_id.cocitref1', 'cocitref2': '$_id.cocitref2'},
        'count': {'$sum': 1}, 'conts': {
          '$push': {
            'cont_id': '$_id.cont_id', 'pub_id': '$cont.pub_id',
            'frag_num': '$cont.frag_num', }, }, }},
    {'$sort': {'count': -1, '_id': 1}},
    {'$project': {
      '_id': 0, 'cocitpair': {
        'ref1': '$_id.cocitref1', 'ref2': '$_id.cocitref2'},
      'count': '$count', 'conts': '$conts', }},
  ]
  if topn:
    pipeline += [{'$limit': topn}]

  # _logger.debug('pipeline: %s', pipeline)

  contexts = mdb.contexts
  curs = contexts.aggregate(pipeline)
  # out = [doc async for doc in curs]
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


async def _req_top_detail_bund_refauthors(
  request: web.Request
) -> web.StreamResponse:
  app = request.app
  mdb = app['db']

  topn = getreqarg_topn(request)

  pipeline = [
    {'$match': {'exact': {'$exists': 1}}},
    {'$project': {
      'prefix': 0, 'suffix': 0, 'exact': 0, 'linked_papers_topics': 0,
      'linked_papers_ngrams': 0}},
    {'$unwind': '$bundles'},
    {'$match': {'bundles': {'$ne': 'nUSJrP'}}},
    {'$lookup': {
      'from': 'bundles', 'localField': 'bundles', 'foreignField': '_id',
      'as': 'bun'}},
    {'$unwind': '$bun'},
    {'$unwind': '$bun.authors'},
    {'$group': {
      '_id': {'author': '$bun.authors', 'bund': '$bundles'},
      'cits': {'$addToSet': '$_id'}, 'cits_all': {'$sum': 1},
      'pubs': {'$addToSet': '$pub_id'},
      'bunds': {'$addToSet': {
        '_id': '$bun._id', 'total_cits': '$bun.total_cits',
        'total_pubs': '$bun.total_pubs'}},}},
    {'$unwind': '$bunds'},
    {'$group': {
      '_id': '$_id.author',
      'cits_all': {'$sum': '$cits_all'},
      'cits': {'$push': '$cits'}, 'pubs': {'$push': '$pubs'},
      'bunds': {'$push': {
        '_id': '$bunds._id', 'cnt': '$cits_all',
        'total_cits': '$bunds.total_cits', 'total_pubs': '$bunds.total_pubs'}}, }},
    {'$project': {
      '_id': 0, 'author': '$_id', 'cits_all': '$cits_all',
      'cits': {'$size': {'$reduce': {
        'input': '$cits', 'initialValue': [],
        'in': {'$setUnion': ['$$value', '$$this']}}}, },
      'pubs': {'$size': {'$reduce': {
        'input': '$pubs', 'initialValue': [],
        'in': {'$setUnion': ['$$value', '$$this']}}}},
      'bunds_cnt': {'$size': '$bunds'},
      'total_cits': {'$sum': '$bunds.total_cits'},
      'total_pubs': {'$sum': '$bunds.total_pubs'},
      'bunds': {'$map': {
        'input': '$bunds', 'as': 'b',
        'in': {'bund': '$$b._id', 'cnt': '$$b.cnt'},}}, }},
    {'$sort': {'cits_all': -1, 'cits': -1, 'bunds_cnt': -1, 'pubs': -1, 'author': 1}},
  ]

  if topn:
    pipeline += [{'$limit': topn}]

  contexts:Collection = mdb.contexts
  out = []
  async for row in contexts.aggregate(pipeline):
    out.append(row)

  return json_response(out)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest

import server_cirtec


def test_test():
  pass


def test_conf():
  conf = server_cirtec._load_conf()
  assert conf['mongodb']['uri']
  assert conf['mongodb']['db']
  assert conf['srv_run_args']['port']


async def init_server(aiohttp_client):
  server_cirtec._init_logging()
  app, conf = server_cirtec.create_srv()
  client = await aiohttp_client(app)
  return client


async def test_frags(aiohttp_client):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/frags/')
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) == 5


async def test_frags_publications(aiohttp_client):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/frags/publications/')
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) >= 24


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_cocitauthors(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/cocitauthors/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_cocitauthors_cocitauthors(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/cocitauthors/cocitauthors/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_cocitauthors_ngramm(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/cocitauthors/ngramms/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_cocitauthors_topics(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/cocitauthors/topics/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_topics(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/topics/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_topics_topics(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/topics/topics/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_topics_cocitauthors(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/topics/cocitauthors/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_topics_ngramms_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/topics/ngramms/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn,crossn', [(5, 10)])
async def test_frags_topics_ngramms_topn_crossn(
  aiohttp_client, topn:int, crossn:int
):
  client = await init_server(aiohttp_client)
  rsp = await client.get(
    '/cirtec/frags/topics/ngramms/',
    params=dict(topn=str(topn), topn_crpssgramm=str(crossn)))
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) == topn
  for v in jrsp.values():
    assert 0 < len(v['crossgrams']) <= crossn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_ngramm_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/ngramms/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_ngramm_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/ngramms/ngramms/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_ngramm_cocitauthors_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/ngramms/cocitauthors/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) <= topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_ngramm_topics_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/frags/ngramms/topics/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_top_cocitauthors_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/top/cocitauthors/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == list
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_top_ngramm_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/top/ngramms/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == list
  if topn:
    assert len(jrsp) == topn


@pytest.mark.parametrize('topn', [None, 5])
async def test_top_topics_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get('/cirtec/top/topics/', **kwd)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == list
  if topn:
    assert len(jrsp) == topn

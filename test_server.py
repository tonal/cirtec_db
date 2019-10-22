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


async def test_frags_cocitauthors(aiohttp_client):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/frags/cocitauthors/')
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) >= 24


@pytest.mark.parametrize('topn', [5])
async def test_frags_cocitauthors_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  rsp = await client.get(
    '/cirtec/frags/cocitauthors/', params=dict(topn=str(topn)))
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) == 5


async def test_frags_cocitauthors_ngramm(aiohttp_client):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/frags/cocitauthors/ngramms/')
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) >= 10


@pytest.mark.parametrize('topn', [5])
async def test_frags_cocitauthors_ngramm_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  rsp = await client.get(
    '/cirtec/frags/cocitauthors/ngramms/', params=dict(topn=str(topn)))
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) == 5


async def test_frags_topics(aiohttp_client):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/frags/topics/')
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) >= 20


@pytest.mark.parametrize('topn', [5])
async def test_frags_topics_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  rsp = await client.get(
    '/cirtec/frags/topics/', params=dict(topn=str(topn)))
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) == 5


async def test_frags_ngramm(aiohttp_client):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/frags/ngramm/')
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) == 10


@pytest.mark.parametrize('topn', [5])
async def test_frags_ngramm_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  rsp = await client.get(
    '/cirtec/frags/ngramm/', params=dict(topn=str(topn)))
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) == 5


async def test_top_cocitauthors(aiohttp_client):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/top/cocitauthors/')
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == list
  assert len(jrsp) >= 24


@pytest.mark.parametrize('topn', [5])
async def test_top_cocitauthors_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  rsp = await client.get(
    '/cirtec/top/cocitauthors/', params=dict(topn=str(topn)))
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == list
  assert len(jrsp) == 5


async def test_top_ngramm(aiohttp_client):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/top/ngramm/')
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == list
  assert len(jrsp) >= 10


@pytest.mark.parametrize('topn', [5])
async def test_top_ngramm_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/top/ngramm/', params=dict(topn=str(topn)))
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == list
  assert len(jrsp) == 5


async def test_top_topics(aiohttp_client):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/top/topics/')
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == list
  assert len(jrsp) >= 10


@pytest.mark.parametrize('topn', [5])
async def test_top_topics_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  rsp = await client.get('/cirtec/top/topics/', params=dict(topn=str(topn)))
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == list
  assert len(jrsp) == 5

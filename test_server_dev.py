#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest

import server_cirtec_dev
import server_utils
import utils4tests


def test_test():
  pass


def test_conf():
  conf = server_cirtec_dev._load_conf()
  utils4tests.test_conf(conf)


async def init_server(aiohttp_client):
  server_utils._init_logging()
  app, conf = server_cirtec_dev.create_srv()
  client = await aiohttp_client(app)
  return client


@pytest.mark.parametrize('topn', [None, 5])
async def test_top_refsbindles(aiohttp_client, topn):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(client, '/cirtec/top/ref_bindles/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_top_refauthors(aiohttp_client, topn):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(client, '/cirtec/top/ref_authors/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags(aiohttp_client, topn):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(client, '/cirtec/frags/', topn)


async def test_frags_publications(aiohttp_client):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(client, '/cirtec/frags/publications/', None)
  assert len(jrsp) >= 24


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_cocitauthors(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(client, '/cirtec/frags/cocitauthors/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_cocitauthors_cocitauthors(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/frags/cocitauthors/cocitauthors/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_publ_cocitauthors_cocitauthors(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/publ/cocitauthors/cocitauthors/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_cocitauthors_ngramm(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/frags/cocitauthors/ngramms/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_cocitauthors_topics(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/frags/cocitauthors/topics/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_topics(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(client, '/cirtec/frags/topics/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_topics_topics(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/frags/topics/topics/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_publ_topics_topics(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(client, '/cirtec/publ/topics/topics/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_topics_cocitauthors(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/frags/topics/cocitauthors/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_topics_ngramms_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/frags/topics/ngramms/', topn)


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
  jrsp = await utils4tests.req_tipn(client, '/cirtec/frags/ngramms/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_ngramm_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/frags/ngramms/ngramms/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_publ_ngramm_ngramm_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/publ/ngramms/ngramms/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_ngramm_cocitauthors_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/frags/ngramms/cocitauthors/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_frags_ngramm_topics_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec/frags/ngramms/topics/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_top_cocitauthors_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(client, '/cirtec/top/cocitauthors/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_top_ngramm_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(client, '/cirtec/top/ngramms/', topn)


@pytest.mark.parametrize('topn', [None, 5])
async def test_top_topics_topn(aiohttp_client, topn:int):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(client, '/cirtec/top/topics/', topn)

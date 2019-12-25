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
@pytest.mark.parametrize('url', [
  '/cirtec_dev/top/ref_bundles/',
  '/cirtec_dev/top/ref_authors/',
  '/cirtec_dev/pubs/ref_authors/',
  '/cirtec_dev/ref_auth_bund4ngramm_tops/',
  '/cirtec_dev/ref_bund4ngramm_tops/',
  '/cirtec_dev/ref_auth4ngramm_tops/',
  '/cirtec_dev/frags/',
  '/cirtec_dev/frags/cocitauthors/',
  '/cirtec_dev/frags/cocitauthors/cocitauthors/',
  '/cirtec_dev/publ/cocitauthors/cocitauthors/',
  '/cirtec_dev/frags/cocitauthors/ngramms/',
  '/cirtec_dev/frags/cocitauthors/topics/',
  '/cirtec_dev/frags/topics/',
  '/cirtec_dev/frags/topics/topics/',
  '/cirtec_dev/publ/topics/topics/',
  '/cirtec_dev/frags/topics/cocitauthors/',
  '/cirtec_dev/frags/topics/ngramms/',
  '/cirtec_dev/frags/ngramms/',
  '/cirtec_dev/frags/ngramms/ngramms/',
  '/cirtec_dev/publ/ngramms/ngramms/',
  '/cirtec_dev/frags/ngramms/cocitauthors/',
  '/cirtec_dev/frags/ngramms/topics/',
  '/cirtec_dev/top/cocitauthors/',
  '/cirtec_dev/top/ngramms/',
  '/cirtec_dev/top/topics/',
  '/cirtec_dev/top/cocitrefs/,
  '/cirtec_dev/top/cocitrefs/cocitrefs/',
])
async def test_get_urls(aiohttp_client, url:str, topn:int):
  client = await init_server(aiohttp_client)
  rsp = await utils4tests.client_get(client, url, topn)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  if topn:
    assert len(jrsp) == topn


async def test_frags_publications(aiohttp_client):
  client = await init_server(aiohttp_client)
  jrsp = await utils4tests.req_tipn(
    client, '/cirtec_dev/frags/publications/', None)
  assert len(jrsp) >= 24


@pytest.mark.parametrize('topn,crossn', [(5, 10)])
async def test_frags_topics_ngramms_topn_crossn(
  aiohttp_client, topn:int, crossn:int
):
  client = await init_server(aiohttp_client)
  rsp = await client.get(
    '/cirtec_dev/frags/topics/ngramms/',
    params=dict(topn=str(topn), topn_crpssgramm=str(crossn)))
  assert 200 == rsp.status
  jrsp = await rsp.json()
  assert type(jrsp) == dict
  assert len(jrsp) == topn
  for v in jrsp.values():
    assert 0 < len(v['crossgrams']) <= crossn

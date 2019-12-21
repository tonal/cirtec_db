# -*- codong: utf-8 -*-
from typing import Optional


def test_conf(conf):
  assert conf['mongodb']['uri']
  assert conf['mongodb']['db']
  assert conf['srv_run_args']['port']


async def req_tipn(client, url:str, topn:Optional[int]):
  rsp = await client_get(client, url, topn)
  assert 200 == rsp.status
  jrsp = await rsp.json()
  if topn:
    assert len(jrsp) == topn
  return jrsp


async def client_get(client, url:str, topn:Optional[int]):
  if topn:
    kwd = dict(params=dict(topn=str(topn)))
  else:
    kwd = {}
  rsp = await client.get(url, **kwd)
  return rsp


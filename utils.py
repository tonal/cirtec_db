# -*- codong: utf-8 -*-
from pathlib import Path
from typing import Optional

import yaml


def load_config():
  path = Path(__file__).resolve()
  conf = yaml.full_load(
    path.with_name('config.yaml').open('r', encoding='utf-8'))
  return conf


def norm_spaces(v:str, *, default:str=None) -> Optional[str]:
  if not v:
    return default
  res = ' '.join(v.split())
  return res
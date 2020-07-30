# -*- codong: utf-8 -*-
import logging
from pathlib import Path
from typing import Optional

import yaml


def get_logger_ord():
  logger = logging.getLogger('cirtec_fastapi')
  return logger


def get_logger_dev():
  logger = logging.getLogger('cirtec_dev_fastapi')
  return logger


def _load_config():
  path = Path(__file__).resolve()
  conf = yaml.full_load(
    path.with_name('config.yaml').open('r', encoding='utf-8'))
  return conf


def load_config_dev():
  conf = _load_config()
  return conf['dev']


def load_config_ord():
  conf = _load_config()
  return conf


def norm_spaces(v:str, *, default:str=None) -> Optional[str]:
  if not v:
    return default
  res = ' '.join(v.split())
  return res

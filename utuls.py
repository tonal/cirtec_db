# -*- codong: utf-8 -*-
from pathlib import Path

import yaml


def load_config():
  path = Path(__file__).resolve()
  conf = yaml.full_load(
    path.with_name('config.yaml').open('r', encoding='utf-8'))
  return conf

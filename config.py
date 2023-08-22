#
# BSD 3-Clause License
#
# Copyright (c) 2023 Fred W6BSD
# All rights reserved.
#
#

import os
import logging
import sys

import yaml

CONFIG_FILENAME = "wsprstats.yaml"
CONFIG_LOCATIONS = ['/etc', '~/.local', '.']

class Config:
  _instance = None
  config_data = None
  def __new__(cls, *args, **kwargs):
    # pylint: disable=unused-argument
    if cls._instance is None:
      cls._instance = super(Config, cls).__new__(cls)
      cls._instance.config_data = {}
    return cls._instance

  def __init__(self, config_filename=None):
    self.log = logging.getLogger('Config')
    if self.config_data:
      return

    for path in CONFIG_LOCATIONS:
      filename = os.path.expanduser(os.path.join(path, CONFIG_FILENAME))
      if os.path.exists(filename):
        self.log.debug('Reading config file: %s', filename)
        try:
          self.config_data = self._read_config(filename)
        except ValueError as err:
          self.log.error('Configuration error "%s"', err)
          sys.exit(os.EX_CONFIG)
        return
    self.log.error('Configuration file "%s" not found', CONFIG_FILENAME)
    sys.exit(os.EX_CONFIG)

  def __getitem__(self, key):
    if key not in self.config_data:
      raise KeyError
    return self.config_data[key]

  def __getattr__(self, attr):
    return self[attr]

  @staticmethod
  def _read_config(filename):
    with open(filename, 'r', encoding='utf-8') as confd:
      configuration = yaml.safe_load(confd)
    return configuration

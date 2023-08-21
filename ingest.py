#!/usr/bin/env python3
#
import gzip
import logging
import os
import re
import time

from argparse import ArgumentParser
from datetime import datetime
from datetime import timedelta
from functools import partial
from urllib.parse import urljoin
from urllib.request import URLError
from urllib.request import urlopen
from urllib.request import urlretrieve

import geo

from dbutils import DBConnect
from dbutils import DB_NAME
from dbutils import WSPR_PATH
from dbutils import create_db

logging.basicConfig(
  format='%(asctime)s %(name)s:%(lineno)d %(levelname)s - %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
  level=logging.getLevelName(os.getenv('LEVEL', 'INFO'))
)

TMP_DIR = WSPR_PATH
BUF_SIZE = 2 << 12

WSPR_ARCHIVE = "http://wsprnet.org/archive/"
WSPR_FILE = "wsprspots-{:4d}-{:02d}.csv.gz"

INSERT = "INSERT INTO wspr VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

BULK_LENGTH = 1000

class Timer:
  def __init__(self, title="Imported", nb_lines=0):
    self.nb_lines = nb_lines
    self.title = title
    self.start = None

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    elapsed = time.time()-self.start
    milliseconds = round(elapsed % 1, 3) * 1000
    minute, sec = divmod(int(elapsed), 60)
    hour, minute = divmod(minute, 60)
    message = [f'{self.title} in [']
    if hour:
      message.append(f'{hour} hour, ')
    if minute:
      message.append(f'{minute} minute, ')
    message.append(f'{sec} seconds, and {milliseconds} milliseconds] ')
    if self.nb_lines:
      rate = self.nb_lines / elapsed
      message.append(f' or {rate:.0f} items per second')
    logging.info(''.join(message))


def get_size(filename):
  if not os.path.exists(filename):
    return 0
  return os.stat(filename).st_size


def download_archive(path, month=None, force=False):
  today = datetime.now()
  if not month:
    month = today.month
  target_file = WSPR_FILE.format(today.year, month)
  target_path = os.path.join(path, target_file)
  url = urljoin(WSPR_ARCHIVE, target_file)

  try:
    with urlopen(url) as resp:
      if not force and resp.length <= get_size(target_path):
        logging.info('No new data in %s', url)
        return None
      logging.info('Downloading %s', url)
      with open(target_path, 'wb') as fdout:
        for buffer in iter(partial(resp.read, BUF_SIZE), b''):
          fdout.write(buffer)
  except URLError as err:
    logging.info('HTTP Error: "%s"', err)
    return None

  return target_path


def read_spots(filename, start_id=0):
  with gzip.open(filename, 'rt', encoding='UTF-8') as zfd:
    for line in zfd:
      fields = line.rstrip().split(',')
      if int(fields[0]) <= start_id:
        continue
      fields.extend(geo.grid2latlon(fields[3])) # tx
      fields.extend(geo.grid2latlon(fields[7])) # rx
      yield fields


def wspr_import(db_name, filename, start_id):
  counter = 0
  wspot = read_spots(filename, start_id)
  with DBConnect(db_name) as conn:
    cursor = conn.cursor()
    try:
      while True:
        bulk = []
        for _ in range(BULK_LENGTH):
          bulk.append(wspot.__next__())
        cursor.executemany(INSERT, bulk)
        counter += len(bulk)
    except StopIteration:
      if bulk:
        cursor.executemany(INSERT, bulk)
        counter += len(bulk)
  logging.info('Records processed %d', counter)


def main():
  parser = ArgumentParser(description="Injest WSPR data.")
  parser.add_argument("-F", "--force", action="store_true", default=False,
                      help="Force download and ingestion")
  parser.add_argument("-f", "--filename", help="WSPR data file (gziped)")
  parser.add_argument("-m", "--month", type=int, help="Month to download")
  opts = parser.parse_args()

  if opts.filename:
    filename = opts.filename
  else:
    with Timer('Download archive'):
      filename = download_archive(TMP_DIR, month=opts.month, force=opts.force)

  if not filename or not os.path.exists(filename):
    logging.warning('Nothing to import')
    return

  logging.info('Database: %s', DB_NAME)
  if not os.path.exists(DB_NAME):
    create_db(DB_NAME)
    last_id = 0
  else:
    logging.info('Counting records')
    with DBConnect(DB_NAME) as conn:
      curs = conn.cursor()
      result = curs.execute('SELECT MAX(spot_id) AS last_id FROM wspr').fetchone()
      last_id = 0 if result['last_id'] is None else int(result['last_id'])
    logging.info('Last id: %d', last_id)

  logging.info('Importing %s', filename)
  with Timer('Import WSPR'):
    wspr_import(DB_NAME, filename, start_id=last_id)

  logging.info(geo.grid2latlon.cache_info())


if __name__ == "__main__":
  main()

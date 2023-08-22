
import sqlite3
import os

SQL_TABLE = """
CREATE TABLE IF NOT EXISTS wspr
(
  spot_id INTEGER PRIMARY KEY,
  time TIMESTAMP,
  tx_call TEXT,
  tx_grid TEXT,
  snr INTEGER,
  frequency REAL,
  rx_call TEXT,
  rx_grid TEXT,
  power INTEGER,
  drift INTEGER,
  distance REAL,
  azimuth REAL,
  band INTEGER,
  version TEXT,
  code INTEGER,
  tx_lat REAL,
  tx_lon REAL,
  rx_lat REAL,
  rx_lon REAL
);
CREATE INDEX idx_time ON wspr (time);
CREATE INDEX idx_tx_call ON wspr (tx_call);
CREATE INDEX idx_rx_call ON wspr (rx_call);
CREATE INDEX idx_band ON wspr (band);
PRAGMA synchronous = EXTRA;
PRAGMA journal_mode = WAL;
"""

class DBConnect:

  def __init__(self, db_name, timeout=15):
    self.db_name = db_name
    self.timeout = timeout
    self.conn = None

  def __enter__(self):
    return self.connect()

  def __exit__(self, *args):
    self.conn.commit()
    self.conn.close()

  def connect(self):
    self.conn = sqlite3.connect(self.db_name, timeout=self.timeout, isolation_level="DEFERRED",
                                detect_types=sqlite3.PARSE_DECLTYPES)
    self.conn.row_factory = sqlite3.Row
    return self.conn


def create_db(db_name):
  with DBConnect(db_name) as conn:
    curs = conn.cursor()
    curs.executescript(SQL_TABLE)

#!/usr/bin/env python3.9

import argparse
import logging
import os
import re

from datetime import datetime, timedelta
from subprocess import Popen, PIPE

import matplotlib.pyplot as plt
import numpy as np

from matplotlib.colors import LinearSegmentedColormap
from mpl_toolkits.basemap import Basemap

from config import Config
from dbutils import DBConnect

TIME_INCREMENT = 3600/2
KEEP_DAYS = 10                  # Maximum number of days we keep the images before they get purged
FILE_DATE_FORMAT = '%Y%m%d%H%M'


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

def load_data(db_name, start, end):
  logging.debug('Reading WSPR data...')
  data = []

  sql_req = 'SELECT rx_lat, tx_lon, tx_lat, tx_lon FROM wspr WHERE time >= ? and time < ?'
  with DBConnect(db_name) as conn:
    conn.row_factory = None
    curs = conn.cursor()
    result = curs.execute(sql_req, (start.timestamp(), end.timestamp()))
    for fields in result:
      data.append(fields[2:4])
  return np.array(data)


def mk_colormap():
  # colors = [(.0, '#001177'), (.20, '#aaaa00'), (.66, '#ffff00'), (1, '#993300')]
  colors = [(.0, '#001155'), (.02, '#99aaaa'), (.4, '#ffff00'), (1, '#ff0000')]
  cmap_name = 'my_cmap'
  n_bins = 19
  return LinearSegmentedColormap.from_list(cmap_name, colors, N=n_bins, gamma=.9)


def plot_map(data, filename, end_date):
  logging.debug('Plotting: %d datapoints', data.shape)
  fig = plt.figure(figsize=(12, 8))
  fig.text(.02, .02, f'Date/Time: {end_date.strftime("%c")} UTC', fontsize=12)
  fig.text(.72, .02, f'Worldwide HF activity - © {datetime.now().year} By W6BSD')

  bmap = Basemap(projection='merc', resolution='c',
                 llcrnrlat=-70, urcrnrlat=77, llcrnrlon=-180, urcrnrlon=180)
  bmap.drawcoastlines(color='lightblue', linewidth=.5)
  bmap.drawcountries(color='yellow', linewidth=.2)
  bmap.nightshade(end_date, color='#111111', alpha=.3)

  lat_bins = np.linspace(-70, 77, int(70/1.3))
  lon_bins = np.linspace(-180, 180, int(180/2.6))
  lon_bins_2d, lat_bins_2d = np.meshgrid(lon_bins, lat_bins)
  x_s, y_s = bmap(lon_bins_2d, lat_bins_2d)

  density = np.histogram2d(data[:,0], data[:,1], [lat_bins, lon_bins])[0]
  density = np.hstack((density, np.zeros((density.shape[0], 1))))
  density = np.vstack((density, np.zeros((density.shape[1]))))

  plt.pcolormesh(x_s, y_s, density, cmap=mk_colormap(), shading='gouraud')

  ax = plt.gca()

  if int(end_date.strftime('%w')) in (0, 6):
    ax.set_xlabel(f'{end_date.strftime("%A")}', fontsize=14, fontweight='bold',
                  color='w', backgroundcolor='k')
  else:
    ax.set_xlabel(f'{end_date.strftime("%A")}', fontsize=14)

  dmax = np.max(density)
  cbar = plt.colorbar(orientation='vertical', shrink=0.83, aspect=15, fraction=0.09, pad=0.02,
                      ticks=[0, dmax / 2, dmax])
  cbar.ax.set_yticklabels(['low', 'med', 'high'], fontsize=12)
  cbar.set_label('Activity density', size=14)

  logging.info('Save figure %s', filename)
  plt.title('Worldwide HF Activity (WSPR)', fontweight='bold', fontsize=20, pad=20)
  plt.savefig(filename, transparent=False, dpi=100)
  plt.close()


def animate(src, video_file):
  config = Config()
  logfile = '/tmp/heat_video.log'
  tmp_file = f"{video_file}-{os.getpid()}.mp4"
  input_files = os.path.join(src, 'world-*.png')
  in_args = f'-y -framerate 8 -pattern_type glob -i {input_files}'.split()
  ou_args = '-c:v libx264 -pix_fmt yuv420p -vf scale=800:600'.split()
  cmd = [config.ffmpeg, *in_args, *ou_args, tmp_file]
  logging.info('Writing ffmpeg output in %s', logfile)
  logging.info("Saving %s video file", tmp_file)
  with open(logfile, "a", encoding='ascii') as err:
    err.write(' '.join(cmd))
    err.write('\n\n')
    err.flush()
    with Popen(cmd, shell=False, stdout=PIPE, stderr=err) as proc:
      proc.wait()
    if proc.returncode != 0:
      logging.error('Error generating the video file')
      return
    logging.info('Move %s to %s', tmp_file, video_file)
    os.rename(tmp_file, video_file)


def gen_map(start_date, end_date, filename):
  config = Config()
  data = load_data(config.db_name, start_date, end_date)
  if data.size > 0:
    plot_map(data, filename, end_date)
  else:
    logging.warning('Empty dataset for %s', start_date.strftime('%Y-%m-%d %H:%M'))


def purge_oldfiles(wdir, days=KEEP_DAYS):
  end_date = datetime.combine(datetime.now(), datetime.min.time()) - timedelta(days=days)
  get_date = re.compile(r'world-(\d+).png').match

  for filename in os.listdir(wdir):
    if not (match := get_date(filename)):
      continue
    date = datetime.strptime(match.group(1), FILE_DATE_FORMAT)
    if date < end_date:
      os.unlink(os.path.join(wdir, filename))
      logging.info('Delete: %s', filename)


def video(opts):
  dest_dir = opts.workdir
  end_date = datetime.now()
  start_date = datetime.combine(end_date.date(), datetime.min.time()) - timedelta(days=opts.days)

  purge_oldfiles(dest_dir)
  while start_date <= end_date:
    end = start_date + timedelta(seconds=TIME_INCREMENT)
    filename = "world-" + start_date.strftime(FILE_DATE_FORMAT) + '.png'
    filename = os.path.join(dest_dir, filename)
    if not os.path.exists(filename):
      gen_map(start_date, end, filename)
    start_date = end

  video_file = os.path.join(opts.video_dir, 'world.mp4')
  animate(dest_dir, video_file)


def image(opts):
  dest_dir = opts.target_dir
  start_date = datetime.strptime(opts.date, '%Y%m%d%H')
  end_date = start_date + timedelta(seconds=3600)
  filename = "world-" + start_date.strftime(FILE_DATE_FORMAT) + '.png'
  filename = os.path.join(dest_dir, filename)
  gen_map(start_date, end_date, filename)


def main():
  config = Config()
  parser = argparse.ArgumentParser(description='DXCC trafic animation')
  subparsers = parser.add_subparsers(required=True)
  p_video = subparsers.add_parser('video')
  p_video.set_defaults(func=video)
  p_video.add_argument('-d', '--days', type=int, default=8,
                       help='Number of days')
  p_video.add_argument('-w', '--workdir', default=config.work_path,
                       help='Working directory')
  p_video.add_argument('-v', '--video-dir', default=config.work_path,
                       help='Directory to store the videos')
  p_image = subparsers.add_parser('image')
  p_image.set_defaults(func=image)
  p_image.add_argument('-d', '--date', required=True, help='Heatmap date [YYYYMMDDHH]')
  p_image.add_argument('-t', '--target-dir', default='/tmp', help='Directory to store the image')

  opts = parser.parse_args()
  opts.func(opts)


if __name__ == "__main__":
  main()

import requests
import sqlite3
import click
import os
import urllib
import json
import tqdm
import subprocess
import glob
import re
import xml.etree.ElementTree as ElementTree
import tarfile

BDSS_URL = 'https://developer.uspto.gov/products/bdss/get/ajax'
BDSS_DATA = {
  # patent grants
  'name': 'PTGRDT',
  'fromDate': "2002-01",
  'toDate': "2018-02"
}


class PatentCatalogue(object):
  def __init__(self, db_path, base):
    self.db = sqlite3.connect(db_path)

    self.base = base
    self.cache_dir = os.path.join(self.base, 'cache')
    self.patent_dir = os.path.join(self.base, 'patents')
    self.release_dir = os.path.join(self.base, 'releases')
    for dirpath in [self.cache_dir, self.patent_dir, self.release_dir]:
      if not os.path.exists(dirpath):
        os.mkdir(dirpath)

    self.releases = {}

  def init_db(self):
    c = self.db.cursor()
    c.execute('''CREATE TABLE releases (id integer primary key, name text, url text, downloaded boolean, extracted boolean)''')
    c.execute('''CREATE TABLE patent (id integer primary key, filename text unique, release_id integer, type varchar(15), title text, patent_reference text, extracted boolean)''')
    c.execute('''CREATE TABLE image (filename text primary key, patent_id integer, tweeted boolean)''')

    self.db.commit()

  def download_missing(self):
    c = self.db.cursor()
    to_download = c.execute('SELECT * from releases WHERE downloaded = 0')
    for rel in to_download:
      self.download(rel)

  def download(self, row):
    release_id, name, url, downloaded, _ = row
    if downloaded:
      print 'already downloaded'
      return True

    r = requests.get(url, stream=True)

    dest_file = os.path.join(self.cache_dir, name)
    dest_file_temp = dest_file + '.tmp'

    if not os.path.exists(dest_file):
      with tqdm.tqdm(desc=name, unit_scale=True, unit='B', total=int(r.headers['content-length'])) as progress:
        with open(dest_file_temp, 'wb') as fd:
          for chunk in r.iter_content(chunk_size=4096):
            fd.write(chunk)
            progress.update(4096)

      os.rename(dest_file_temp, dest_file)

    c = self.db.cursor()
    c.execute('UPDATE releases SET downloaded = 1 WHERE id = ?', [release_id])
    self.db.commit()

  def find_release_by_name(self, name):
    c = self.db.cursor()
    to_download = c.execute('SELECT * from releases WHERE name LIKE ?', ['%%%s%%' % name])
    row = c.fetchone()
    if not row:
      print 'no such release matching', name
      return None

    return row

  def extract_missing(self):
    c = self.db.cursor()
    to_download = c.execute('SELECT * from releases WHERE extracted = 0 AND downloaded = 1')
    for row in to_download:
      print row
      self.extract(row)

  def load_patents_for_release(self, release_id, new_patents):
    c = self.db.cursor()
    for patent_fname in tqdm.tqdm(new_patents, desc='import'):
      patdir = os.path.join(self.patent_dir, patent_fname)
      tree = ElementTree.ElementTree()
      dest = os.path.join(patdir, patent_fname + '.XML')
      print dest
      if not os.path.exists(dest):
        print 'warning, no', dest
        continue

      tree.parse(dest)

      root = tree.getroot()
      appref = root.find('./us-bibliographic-data-grant/application-reference')
      patent_type = appref.get('appl-type')

      appnum_country = appref.find('./document-id/country').text
      appnum = appref.find('./document-id/doc-number').text

      title = root.find('./us-bibliographic-data-grant/invention-title').text

      c.execute('INSERT INTO patent VALUES (null, ?, ?, ?, ?, ?, 1)', [patent_fname, release_id, patent_type, title, appnum_country + appnum])
      patent_id = c.lastrowid

      imgs = root.findall('./drawings/figure/img')

      for img in imgs:
        c.execute('INSERT INTO image VALUES (?, ?, 0)', [img.get('file'), patent_id])

      self.db.commit()

  def extract(self, row):
    release_id, name, url, downloaded, _ = row
    patent_release_dir = os.path.join(self.release_dir, 'release-%d' % release_id)

    if not os.path.exists(patent_release_dir):
      os.mkdir(patent_release_dir)

    # extract archive tar to get a collection of patents
    print 'extracting release %s' % name
    with tarfile.open(os.path.join(self.cache_dir, name), 'r') as tar:
      for tarinfo in tqdm.tqdm(tar, desc='untar'):
        if not tarinfo.isfile():
          continue

        if not os.path.exists(os.path.join(patent_release_dir, os.path.basename(tarinfo.name))):
          # print 'extracting', tarinfo.name, 'to', patent_release_dir
          tar.extract(tarinfo, patent_release_dir)
          os.rename(
            os.path.join(patent_release_dir, tarinfo.name), 
            os.path.join(patent_release_dir, os.path.basename(tarinfo.name)))

    new_patents = []
    for zipname in tqdm.tqdm(glob.glob(os.path.join(patent_release_dir, '*.[zZ][iI][pP]')), desc='unzip'):
      new_patents.append(re.sub(r'.*/(.*)\.ZIP$', r'\g<1>', zipname))
      subprocess.check_call(['unzip', '-qq', '-o', zipname, '-d', self.patent_dir])
      os.remove(zipname)

    c = self.db.cursor()
    c.execute('UPDATE releases SET extracted = 1 WHERE id = ?', [release_id])
    self.db.commit()

    return self.load_patents_for_release(release_id, new_patents)

  def scan_remote_releases(self):
    r = requests.get(BDSS_URL + '?data=' + urllib.quote(json.dumps(BDSS_DATA)))
    j = r.json()
    c = self.db.cursor()
    for remoteFile in j['productFiles']:
      c.execute('''INSERT OR IGNORE INTO releases(
                name, url, downloaded)
                VALUES 
                (?, ?, 0)''',
                [remoteFile['fileName'], remoteFile['fileDownloadUrl']])

    self.db.commit()

@click.group()
@click.pass_context
def cli(ctx):
  need_create = not os.path.exists('catalogue.db')
  ctx.obj['catalogue'] = PatentCatalogue('catalogue.db', '.')

  if need_create:
    ctx.obj['catalogue'].init_db()

@cli.command()
@click.pass_context
def update(ctx):
  ctx.obj['catalogue'].scan_remote_releases()

@cli.command()
@click.pass_context
@click.argument('name', required=False)
def extract(ctx, name):
  cat = ctx.obj['catalogue']
  if not name:
    cat.extract_missing()
  else:
    rel = cat.find_release_by_name(name)
    if rel:
      cat.extract(rel)

@cli.command()
@click.pass_context
@click.argument('name', required=False)
def pull(ctx, name):
  cat = ctx.obj['catalogue']
  if not name:
    cat.download_missing()
  else:
    rel = cat.find_release_by_name(name)
    if rel:
      cat.download(rel)

if __name__ == '__main__':
  cli(obj={})
import requests
import sqlite3
import click
import os
import urllib
import json
import tqdm

BDSS_URL = 'https://developer.uspto.gov/products/bdss/get/ajax'
BDSS_DATA = {
  # patent grants
  'name': 'PTGRDT',
  # 'fromDate': "2002-01",
  # 'toDate': "2018-02"
}


class PatentCatalogue(object):
  def __init__(self, db_path, base):
    self.db = sqlite3.connect(db_path)

    self.base = base
    self.cache_dir = os.path.join(self.base, 'cache')
    self.patent_dir = os.path.join(self.base, 'patents')
    for dirpath in [self.cache_dir, self.patent_dir]:
      if not os.path.exists(dirpath):
        os.mkdir(dirpath)

    self.releases = {}

  def init_db(self):
    c = self.db.cursor()
    c.execute('''CREATE TABLE releases (id integer primary key, name text, url text, downloaded boolean, extracted boolean)''')
    c.execute('''CREATE TABLE patent (filename text primary key, type text, release_id integer, extracted boolean)''')
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

  def extract(self, row):
    release_id, name, url, downloaded, _ = row

    # extract archive tar to get a collection of patents
    subprocess.check_call(['tar',
                          '-C', self.patent_dir,
                          'xf', os.path.join(self.cache_dir, name)])

    c = self.db.cursor()
    c.execute('UPDATE releases SET extracted = 1 WHERE id = ?', [release_id])
    self.db.commit()

    # then for all cat/[patent.zip],
    # extract that which contains all the tifs and xml

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
def pull(ctx, name):
  cat = ctx.obj['catalogue']
  if not name:
    cat.download_missing()
  else:
    rel = cat.find_release_by_name(name)
    if rel:
      rel.download_by_name(name)

if __name__ == '__main__':
  cli(obj={})
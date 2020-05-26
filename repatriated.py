#!/usr/bin/python
# encoding: utf-8

import os
import datetime
import sys
import uuid
import time
import json
import urllib
import zipfile
import codecs
import pygbif
import shutil
import unicodecsv as csv

gbifuser = "chrissvi"
gbifpass = "Taephoh4"

reload(sys)
sys.setdefaultencoding('utf-8')
csv.field_size_limit(sys.maxsize)

os.chdir("/site/gbif/repatriated")

key, details = pygbif.occurrences.download(
    ['country = NO', 'repatriated = TRUE'],
    user = gbifuser,
    pwd = gbifpass,
    email = 'christian.svindseth@nhm.uio.no'
)

def waitfordownload(key):
  attempts = 0
  while attempts < 48:
    dls = pygbif.occurrences.download_list(user = gbifuser, pwd = gbifpass)
    for dl in dls.get('results', []):
      if dl['key'] == key and dl['status'] == 'SUCCEEDED':
        return dl['downloadLink']
    sys.stderr.write("Waiting for download link...\n")
    time.sleep(60 * 10)
  return False

url = waitfordownload(key)

if url:
  try:
    os.makedirs("data")
  except:
    pass
  urllib.urlretrieve (url, "data/raw.zip")
  zf = zipfile.ZipFile("data/raw.zip")
  zf.extractall("data")
  zf.close()
  shutil.move('data/occurrence.txt', 'data/raw.txt')

  # ...why does the file contain NUL bytes?
  os.system('sed -i "s/\\x0//g" data/raw.txt') 

  n = 1
  with codecs.open("data/raw.txt", encoding='utf-8', errors='replace') as f:
    with open('data/occurrence.txt', 'w') as wf:
      reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
      writer = csv.DictWriter(wf, reader.fieldnames + ['dateLastModified'], delimiter='\t')
      writer.writeheader()
      for row in reader:
        notes = [
            row.get('occurrenceRemarks'),
            "http://www.gbif.org/occurrence/" + row.get('gbifID')
        ]
        if not row.get('modified'):
          row['modified'] = str(datetime.date.today())
        if not row.get('dateLastModified'):
          row['dateLastModified'] = str(datetime.date.today())
        row['institutionCode'] = 'GBIF'
        row['collectionCode'] = 'Import'
        row['catalogNumber'] = n
        row['occurrenceRemarks'] = " / ".join(filter(bool, notes))
        if row.get('scientificName'):
          writer.writerow(row)
          n += 1
  date = time.strftime("%Y-%m-%d")
  shutil.move("data/raw.zip", "/var/www/lighttpd/repatriated/%s.zip" % date)
  shutil.make_archive("clean", "zip", "data")
  shutil.move("clean.zip", "/var/www/lighttpd/repatriated/%s-clean.zip" % date)
  shutil.rmtree("data")
  meta = {
      'registeredResources': [
        {
          'dwca': "http://data.gbif.no/repatriated/%s-clean.zip" % date,
          'extensions': [],
          'lastPublished': date,
          'records': n,
          'title': "Repatriated GBIF data, Norway (processed)",
          'type': "OCCURRENCE"
        },
        {
          'dwca': "http://data.gbif.no/repatriated/%s.zip" % date,
          'extensions': [],
          'lastPublished': date,
          'records': n,
          'title': "Repatriated GBIF data, Norway",
          'type': "OCCURRENCE"
          }
        ]
      }
  with open("/var/www/lighttpd/repatriated/repatriated.json", "w") as f:
    f.write(json.dumps(meta))


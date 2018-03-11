import tweepy
import sqlite3
import os
import tempfile
from PIL import Image

import twitter_credentials

auth = tweepy.OAuthHandler(twitter_credentials.consumer_key, twitter_credentials.consumer_secret)
auth.set_access_token(twitter_credentials.access_token_key, twitter_credentials.access_token_secret)

api = tweepy.API(auth)

db = sqlite3.connect('./catalogue.db')
c = db.cursor()

c.execute('SELECT * FROM image INNER JOIN patent ON patent.id = image.patent_id WHERE tweeted=0 ORDER BY random() LIMIT 1')
r = c.fetchone()

fname = os.path.join('patents', r[4], r[0])
title = r[7]

handle, dest_fname = tempfile.mkstemp('.png')
os.close(handle)

print dest_fname
image = Image.open(fname)
image.save(dest_fname)

print api.update_with_media(dest_fname, title)

c.execute('UPDATE image SET tweeted=1 WHERE filename=?', [r[0]])
db.commit()
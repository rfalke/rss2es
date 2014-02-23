
# http://www.elasticsearch.org/blog/unleash-the-clients-ruby-python-php-perl/#python

import json
import feedparser
import shelve
from datetime import datetime
import elasticsearch
import time
from time import mktime
from datetime import datetime

def struct_time_to_iso(struct):
    dt = datetime.fromtimestamp(mktime(struct))
    return dt.isoformat()

def prepare_dates(feed):
    for a in ["published", "created", "updated"]:
        b = a+"_parsed"
        if a in feed.keys() and b in feed.keys():
            if feed[b] is None:
                del feed[a]
                del feed[b]
            else:
                feed[a] = struct_time_to_iso(feed[b])
                del feed[b]
            
class Feed:
    def __init__(self, category, language, url):
        self.category = category
        self.language = language
        self.url = url

def get_date(entry):
    if "updated" in entry.keys():
        return entry["updated"]
    elif "published" in entry.keys():
        return entry["published"]
    elif "created" in entry.keys():
        return entry["created"]
    else:
        return datetime.now()

def fetch_once():
    for feed in feeds:
        url = feed.url
        if status.has_key(url):
            etag, modified, known = status[url]
        else:
            etag, modified, known = None, None,{}
        start=time.time()
        d = feedparser.parse(url, etag=etag, modified=modified)
        new_entries = [x for x in d.entries if x.id not in known]
        print "  %d new items for %s in %.1f sec"%(len(new_entries), url, time.time()-start)
        prepare_dates(d.feed)

        for entry in new_entries:
            prepare_dates(entry)
            entry["combined_date"] = get_date(entry)

            es.index(index="rss2es", doc_type="rss-item", id=entry.id,
                     body={"entry": entry,
                           "category": feed.category,
                           "language": feed.language,
                           "feed": d.feed,
                           "indexed_at": datetime.now()})
            known[entry.id] = 1

        if "etag" in d.keys():
            etag = d.etag
        if "modified" in d.keys():
            modified = d.modified
        status[url]=(etag, modified, known)
        
feeds=[]
feeds.append(Feed("News", "german", 'http://newsfeed.zeit.de/index'))
feeds.append(Feed("News", "german", 'http://www.spiegel.de/index.rss'))
feeds.append(Feed("News", "german", 'http://www.stern.de/feed/standard/all/'))
feeds.append(Feed("News", "german", 'http://www.bild.de/rssfeeds/vw-home/vw-home-16725562,short=1,sort=1,view=rss2.bild.xml'))

feeds.append(Feed("Tech-News", "german", 'http://heise.de.feedsportal.com/c/35207/f/653902/index.rss'))
feeds.append(Feed("Discussion", "english", 'http://www.reddit.com/r/all/.rss'))
feeds.append(Feed("Wikipedia", "german", 'http://de.wikipedia.org/w/index.php?title=Spezial:Letzte_%C3%84nderungen&feed=atom'))


feeds.append(Feed("News", "english", 'http://rss.cnn.com/rss/edition.rss'))
feeds.append(Feed("News", "english", 'http://feeds.bbci.co.uk/news/rss.xml'))
feeds.append(Feed("News", "english", 'http://www.france24.com/en/top-stories/rss'))

feeds.append(Feed("Software", "english", "https://www.kernel.org/feeds/kdist.xml"))
feeds.append(Feed("Software", "english", "https://github.com/jquery/jquery/commits/master.atom"))

es = elasticsearch.Elasticsearch()

status = shelve.open("feed.status")

counter=0
while 1:
    start=time.time()
    print "### start at %s loop=%d"%(time.asctime(), counter)
    counter+=1
    fetch_once()
    print "  # finished after %.1f sec; will sleep now"%(time.time()-start)
    time.sleep(10)

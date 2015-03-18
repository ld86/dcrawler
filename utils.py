import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from time import sleep
from HTMLParser import HTMLParser
from urlparse import urlsplit, urlunsplit

import urllib2  

def transform_and_filter(url):
    scheme, netloc, path, query, fragment = urlsplit(url)
    if netloc not in ['simple.wikipedia.org']:
        return None
    if path.find('/wiki/') != 0:
        return None
    if ':' in path:
        return None
    return urlunsplit([scheme, netloc, path, query, ''])

def fetch_query(url):
    last_error = None
    for i in xrange(3):
        try:
            return (urllib2.urlopen(url, timeout=150).read(), None)
        except Exception as e:
            last_error = e
            sleep(1)
    return (None, last_error)

def extract_anchors(content):
    class AnchorExtractor(HTMLParser):

        def __init__(self, anchors):
            HTMLParser.__init__(self)
            self.anchors = anchors

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == 'a' and 'href' in attrs:
                self.anchors.append(attrs['href'])

    anchors = []
    extractor = AnchorExtractor(anchors)
    extractor.feed(content)
    return anchors

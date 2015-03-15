import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from time import sleep
from HTMLParser import HTMLParser

import urllib2  

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

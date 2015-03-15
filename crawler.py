import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from time import sleep
from threading import RLock
from heapq import heappush, heappop
from utils import fetch_query, extract_anchors
from urlparse import urlsplit, urljoin, urlunsplit

class PageContainer:

    def __init__(self, filter_for_urls):
        self.downloaded = set()
        self.urls = set()
        self.heap = []
        self.lock = RLock()
        self.filter_for_urls = filter_for_urls

    def add_new_urls(self, urls):
        with self.lock:
            for url in urls:
                url = self.filter_for_urls(url)
                if url is not None and url not in self.downloaded and url not in self.urls:
                    self.urls.add(url)
                    heappush(self.heap, (0, url))
            print(len(self.heap))

    def get_next_urls(self, n):
        with self.lock:
            candidates = []
            for i in range(n):
                if len(self.heap) == 0:
                    break
                scheduled, url = heappop(self.heap)
                if url not in self.downloaded:
                    candidates.append((scheduled, url))

            for scheduled, url in candidates:
                heappush(self.heap, (scheduled + 1, url))

            return [url for _, url in candidates]

    def mark_as_downloaded(self, urls):
        with self.lock:
            for url in urls:
                self.downloaded.add(url)

class Master(PageContainer):

    def __init__(self):
        def transform_and_filter(url):
            scheme, netloc, path, query, fragment = urlsplit(url)
            if netloc not in ['simple.wikipedia.org']:
                return None
            if path.find('/wiki/') != 0:
                return None
            if ':' in path:
                return None
            return urlunsplit([scheme, netloc, path, query, ''])

        PageContainer.__init__(self, transform_and_filter)
        self.add_new_urls(['http://simple.wikipedia.org/wiki/'])

class Page:

    def __init__(self, url):
        self.url = url
        self.content = None

    def get_content(self):
        if self.content is None:
            self.content, error = fetch_query(self.url)
            if error:
                raise error
        return self.content

    def get_links(self):
        extracted_anchors = extract_anchors(self.get_content())
        links = [Link(self, anchor) for anchor in extracted_anchors]
        return links

class Link:

    def __init__(self, source, anchor):
        self.source = source
        self.destination = Page(urljoin(self.source.url, anchor))

class Slave:

    def __init__(self, master):
        self.master = master

    def start(self):
        while True:
            urls = self.master.get_next_urls(1)
            for url in urls:
                print(url)
                page = Page(url)
                links = page.get_links()
                self.master.mark_as_downloaded([page.url])
                self.master.add_new_urls([link.destination.url for link in links])
                sleep(30)

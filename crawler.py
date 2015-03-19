import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from time import sleep
from threading import RLock
from heapq import heappush, heappop
from utils import fetch_query, extract_anchors, transform_and_filter
from urlparse import urljoin
import sqlite3
import zlib

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
        self.db = 'slave.db'
        self.master = master
        self.__create_tables()

    def __create_tables(self):
        with sqlite3.connect(self.db) as db:
            db.execute('create table if not exists links(source text, destination text)')
            db.execute('create table if not exists pages(url text, content blob)')

    def __save_page(self, page):
        with sqlite3.connect(self.db) as db:
            db.execute('insert into pages values(?,?)', (page.url, buffer(zlib.compress(page.get_content()))))

    def __save_links(self, links):
        with sqlite3.connect(self.db) as db:
            for link in links:
                destination = transform_and_filter(link.destination.url)
                if destination is not None:
                    db.execute('insert into links values(?,?)', (link.source.url, destination))

    def start(self):
        while True:
            urls = self.master.get_next_urls(10)
            for url in urls:
                print(url)
                try:
                    page = Page(url)
                    self.__save_page(page)

                    links = page.get_links()
                    self.__save_links(links)

                    self.master.mark_as_downloaded([page.url])
                    self.master.add_new_urls([link.destination.url for link in links])
                    sleep(1)
                except urllib2.HTTPError as e:
                    print(e)
                    if e.code == 404
                        continue
                    else:
                        raise e

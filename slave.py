import xmlrpclib
from crawler import Slave
from json import load
from sys import argv

options = load(open(argv[1]))
master_url = 'http://{0}:{1}'.format(options['host'], options['port'])
master = xmlrpclib.ServerProxy(master_url, allow_none=True)
Slave(master).start()

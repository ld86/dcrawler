import xmlrpclib
from crawler import Slave

master = xmlrpclib.ServerProxy('http://localhost:8000', allow_none=True)
Slave(master).start()

from SimpleXMLRPCServer import SimpleXMLRPCServer
from crawler import Master
from json import load
from sys import argv

options = load(open(argv[1]))
server_address = (options['host'], options['port'])
server = SimpleXMLRPCServer(server_address, allow_none=True)
server.register_instance(Master())
server.serve_forever()

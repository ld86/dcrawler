from SimpleXMLRPCServer import SimpleXMLRPCServer
from crawler import Master

server = SimpleXMLRPCServer(('localhost', 8000), allow_none=True)
server.register_instance(Master())
server.serve_forever()

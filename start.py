import paxo_server, multiprocessing, sys

max_failure = sys.argv[1]
host = sys.argv[2]
port_number = sys.argv[3]
address_list = []

for i in xrange(2 * max_failure + 1):
	address_list.append((host, port_number + i))

for i in xrange(2 * max_failure + 1):
	replica = paxo_server.Paxo_server(max_failure, i, address_list)

# TODO
# Create client
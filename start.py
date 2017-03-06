import paxo_server, multiprocessing, sys, paxo_client, os

max_failure = int(sys.argv[1])
host = sys.argv[2]
port_number = int(sys.argv[3])
num_client = int(sys.argv[4])
address_list = []
replicas = []
clients = []
commands = ['one', 'two', 'three', 'four', 'five']

os.system('rm log/*')
os.system('rm chat_log/*')

for i in xrange(2 * max_failure + 1):
	address_list.append((host, port_number + i))

for i in xrange(2 * max_failure + 1):
	replicas.append(paxo_server.Paxo_server(max_failure, i, address_list))
	replicas[i].start()
# TODO
# Create client
for i in xrange(num_client):
	clients.append(paxo_client.Paxo_client(i, host, port_number + 2 * max_failure + 1 + i
		, max_failure, address_list, commands))
	clients[i].start()
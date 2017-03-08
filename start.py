import paxos_server, multiprocessing, sys, paxos_client, os

if __name__ == '__main__':
	max_failure = int(sys.argv[1])
	host = sys.argv[2]
	port_number = int(sys.argv[3])
	num_client = int(sys.argv[4])
	address_list = []
	replicas = []
	clients = []
	commands = ['one', 'two', 'three', 'four', 'five']

	os.system('rm -f log/*')
	os.system('rm -f chat_log/*')

	for i in xrange(2 * max_failure + 1):
		address_list.append((host, port_number + i))

	for i in xrange(2 * max_failure + 1):
		replicas.append(paxos_server.Paxos_server(max_failure, i, address_list))
		replicas[i].start()
	# TODO
	# Create client
	for i in xrange(num_client):
		clients.append(paxos_client.Paxos_client(i, host, port_number + 2 * max_failure + 1 + i
			, max_failure, address_list, commands))
		clients[i].start()
import paxos_server, multiprocessing, sys, paxos_client, os
from config import *

if __name__ == '__main__':
	max_failure = int(sys.argv[1])
	replica_id = int(sys.argv[2])

	server = paxos_server.Paxos_server(max_failure, replica_id, server_addresses)
	server.start()
	
	commands = commands
	for i in xrange(num_client):
		clients.append(paxos_client.Paxos_client(i, client_addresses[i][0], client_addresses[i][1]
			, max_failure, server_addresses, commands))
		clients[i].start()
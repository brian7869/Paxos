import paxos_server, multiprocessing, sys, paxos_client, os
from config import *

if __name__ == '__main__':
	max_failure = int(sys.argv[1])
	num_clients = int(sys.argv[2])
	num_commands = int(sys.argv[3])

	num_servers = 2 * max_failure + 1
	servers = []
	clients = []

	for replica_id in xrange(num_servers):
		os.system('python start_server.py {} {}'.format(str(max_failure), str(replica_id)))

	for client_id in xrange(num_clients):
		os.system('python start_client.py {} {} {}'.format(str(max_failure), str(client_id), str(num_commands)))
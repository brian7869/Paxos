import paxos_server, multiprocessing, sys, paxos_client, os
from config import *

if __name__ == '__main__':
	max_failure = int(sys.argv[1])
	replica_id = int(sys.argv[2])
	can_skip_slot = int(sys.argv[3])

	server = paxos_server.Paxos_server(max_failure, replica_id, server_addresses, can_skip_slot)
	server.start()
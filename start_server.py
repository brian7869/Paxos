import paxos_server, multiprocessing, sys, paxos_client, os
from config import *

def start_server(max_failure, replica_id, can_skip_slot, fail_view_change = 0):
	server = paxos_server.Paxos_server(max_failure, replica_id, server_addresses, can_skip_slot, fail_view_change)
	server.start()
	return server

if __name__ == '__main__':
	max_failure = int(sys.argv[1])
	replica_id = int(sys.argv[2])
	can_skip_slot = int(sys.argv[3])
	fail_view_change = int(sys.argv[4])

	start_server(max_failure, replica_id, can_skip_slot, fail_view_change)
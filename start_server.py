import paxos_server, multiprocessing, sys, paxos_client, os
from config import *

def start_server(max_failure, replica_id, can_skip_slot, fail_view_change = 0):
	server = paxos_server.Paxos_server(max_failure, replica_id, server_addresses, can_skip_slot, fail_view_change)
	server.start()
	return server

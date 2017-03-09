import paxos_server, multiprocessing, sys, paxos_client, os
from config import *

def start_server(max_failure, replica_id):
	server = paxos_server.Paxos_server(max_failure, replica_id, server_addresses)
	server.start()
	return server

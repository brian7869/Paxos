import paxos_server, multiprocessing, sys, paxos_client, os
from config import *
from paxos_utils import command_generator

def start_client(max_failure, client_id, num_commands):
	commands = command_generator(num_commands)
	client = paxos_client.Paxos_client(client_id, client_addresses[client_id][0], client_addresses[client_id][1]
			, max_failure, server_addresses, commands)
	client.start()
	return client

if __name__ == '__main__':
	max_failure = int(sys.argv[1])
	client_id = int(sys.argv[2])
	num_commands = int(sys.argv[3])

	start_client(max_failure, client_id, num_commands)
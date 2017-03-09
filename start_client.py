import paxos_server, multiprocessing, sys, paxos_client, os
from config import *

if __name__ == '__main__':
	max_failure = int(sys.argv[1])
	client_id = int(sys.argv[2])
	num_commands = int(sys.argv[3])

	commands = command_generator(num_commands)
	client = paxos_client.Paxos_client(client_id, client_addresses[client_id][0], client_addresses[client_id][1]
			, max_failure, server_addresses, commands)
	client.start()
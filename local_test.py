import paxos_server, multiprocessing, sys, paxos_client, os, subprocess
from config import *

if __name__ == '__main__':
	max_failure = int(sys.argv[1])
	num_clients = int(sys.argv[2])
	num_commands = int(sys.argv[3])

	num_servers = 2 * max_failure + 1
	processes = []

	os.system('rm -f log/*')
	os.system('rm -f chat_log/*')

	for replica_id in xrange(num_servers):
		processes.append(subprocess.Popen(['python', 'start_server.py', str(max_failure), str(replica_id)] ))

	for client_id in xrange(num_clients):
		subprocess.Popen(['python', 'start_client.py', str(max_failure), str(client_id), str(num_commands)] )

	
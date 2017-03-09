import paxos_server, multiprocessing, sys, paxos_client, os, subprocess, signal
from config import *
from start_server import *
from time import sleep

if __name__ == '__main__':
	max_failure = int(sys.argv[1])
	num_clients = int(sys.argv[2])
	num_commands = int(sys.argv[3])

	num_servers = 2 * max_failure + 1
	processes = []

	def signal_handler(signal, frame):
		for proc in processes:
			proc.terminate()
		sys.exit(0)

	signal.signal(signal.SIGINT, signal_handler)
	os.system('rm -f log/*')
	os.system('rm -f chat_log/*')

	for replica_id in xrange(num_servers):
		processes.append(start_server(max_failure, replica_id))

	for client_id in xrange(num_clients):
		subprocess.Popen(['python', 'start_client.py', str(max_failure), str(client_id), str(num_commands)] )

	sleep(15)

	for i in xrange(max_failure):
		processes[i].terminate()
		sleep(10)

	signal.pause()



import paxos_server, multiprocessing, sys, paxos_client, os, subprocess, signal
from config import *
from start_server import *
from time import sleep

if __name__ == '__main__':
	max_failure = int(sys.argv[1])
	num_clients = int(sys.argv[2])
	num_commands = int(sys.argv[3])
	can_skip_slot = 0
	if len(sys.argv) > 3:
		can_skip_slot = int(sys.argv[4])
	if len(sys.argv) > 4:
		fail_during_view_change = int(sys.argv[5])

	num_servers = 2 * max_failure + 1
	processes = []

	def signal_handler(signal, frame):
		for proc in processes:
			proc.terminate()
		sys.exit(0)

	signal.signal(signal.SIGINT, signal_handler)
	os.system('rm -rf log')
	os.system('rm -rf chat_log')
	os.system('mkdir log')
	os.system('mkdir chat_log')

	for replica_id in xrange(num_servers):
		if replica_id < max_failure and replica_id > 0:
			processes.append(start_server(max_failure, replica_id, can_skip_slot, fail_during_view_change))
		else:
			processes.append(start_server(max_failure, replica_id, can_skip_slot))

	for client_id in xrange(num_clients):
		subprocess.Popen(['python', 'start_client.py', str(max_failure), str(client_id), str(num_commands)] )

	# Trigger first leader crash which will cause a chain reaction
	# sleep(10)
	# processes[0].terminate()

	while True:
		try:
			signal.pause()
		except SystemExit as e:
			pass

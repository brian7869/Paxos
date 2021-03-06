import socket, threading, multiprocessing, datetime, json, os
from paxos_utils import json_spaceless_dump
from multiprocessing import Process
from time import sleep

TIMEOUT = 10
HEARTBEART_CYCLE = 3

# make it possible to skip some sequence number
class Paxo_server(Process):
	def __init__(self, max_failure, replica_id, address_list):
		super(Paxo_server, self).__init__()
		# 0. Initialize internal data
		self.max_failure = max_failure
		self.num_replicas = 2 * max_failure + 1
		self.replica_id = replica_id
		self.address_list = address_list
		self.host = address_list[replica_id][0]
		self.port_number = address_list[replica_id][1]
		
		self.log_filename = 'log/server_{}.log'.format(str(replica_id))
		self.chat_log_filename = 'chat_log/server_{}.chat_log'.format(str(replica_id))
		self.log = {}		# {seq: command}
		self.accepted = {}	# {client_address: [client_seq, leader_num, seq, command, num_of_accepted]}
		self.executed_command_seq = -1
		self.assigned_command_seq = -1
		self.leader_num = -1

		self.num_followers = 0
		self.num_accept = 0

		self.replica_heartbeat = []
		
		self.load_log()
		for i in xrange(self.num_replicas):
			self.replica_heartbeat.append(datetime.datetime.now())

		# Locks, not sure if they are necessary... 
		self.heartbeat_lock = threading.Lock()
		self.leader_lock = threading.Lock()
		
	def run(self):
		# 1. Create new UDP socket (receiver)
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.bind((self.host, self.port_number))

		# 2. Create heartbeat sender and failure detector
		heartbeat_sender_thread = threading.Thread(name="heartbeat_sender_" + str(self.replica_id),
												   target=self.heartbeat_sender, args=())
		failure_detector_thread = threading.Thread(name="failure_detector_" + str(self.replica_id),
												   target=self.failure_detector, args=())
		heartbeat_sender_thread.start()
		failure_detector_thread.start()
		
		# 3. Start receiving message
		while True:
			self.debug_print('wait for message')
			message = sock.recv(65535)
			self.message_handler(message)

	def heartbeat_sender(self):
		heartbeat_message = "Heartbeat {}".format(str(self.replica_id))
		while True:
			self.debug_print("send heartbeat")
			self.broadcast(heartbeat_message)
			sleep(HEARTBEART_CYCLE)

	def failure_detector(self):
		# Modify: self.leader_num
		while True:
			new_live_set = []
			for i in xrange(self.num_replicas):
				if (datetime.datetime.now() - self.replica_heartbeat[i]).total_seconds() < TIMEOUT or i == self.replica_id:
					new_live_set.append(i)
			if self.replica_id == new_live_set[0] and\
				( (self.leader_num % self.num_replicas) not in new_live_set or self.leader_num == -1 ):
				# message = "YouShouldBeLeader {}".format(str(self.replica_id))
				# send_message(self.host, self.port_number, message)
				self.followers = 1
				new_leader_num = self.replica_id + (self.leader_num - (self.leader_num % self.num_replicas)) 
				if new_leader_num < self.leader_num:
					new_leader_num += self.num_replicas
				message = "IAmLeader {}".format(str(new_leader_num))
				self.leader_num = new_leader_num
				self.broadcast(message)
			sleep(TIMEOUT)

	def message_handler(self, message):
		# Possible message
		# From replicas:
		# 0. "Heartbeat <sender_id>"
		# 1. "IAmLeader <leader_num>"
		# 2. "YouAreLeader <sender_id> <committed_log> <[(leader_num, seq, command)]>"
		# 3. "Propose <leader_num> <sequence_num> <Request_message>"
		# 4. "Accept <leader_num> <sequence_num> <Request_message>"
		# From clients:
		# 1. "Request <host> <port_number> <client_seq> <command>"
		# 2. "WhoIsLeader <host> <port_number>"
		type_of_message, rest_of_message = tuple(message.split(' ', 1))

		if type_of_message == "Heartbeat":
			# Modify: self.replica_heartbeat
			sender_id = int(rest_of_message)
			self.replica_heartbeat[sender_id] = datetime.datetime.now()
		elif type_of_message == "IAmLeader":
			# Modify: self.leader_num
			new_leader_num = int(rest_of_message)
			if new_leader_num > self.leader_num:
				leader_id = new_leader_num % self.num_replicas
				response = "YouAreLeader {} {}".format(json_spaceless_dump(self.log)
					, json_spaceless_dump(self.accepted.values()))
				self.send_message(self.address_list[leader_id][0], self.address_list[leader_id][1], response)
				self.leader_num = new_leader_num
		elif type_of_message == "YouAreLeader":
			# TODO
			committed_log, accepted = tuple(rest_of_message.split(' ', 1))
			self.num_followers += 1
			if self.num_followers >= self.max_failure + 1:
				self.leader_num = self.replica_id + (self.leader_num - (self.leader_num % self.num_replicas))

		elif type_of_message == "Propose":
			# Add it to accepted
			leader_num, seq, request_message = tuple(rest_of_message.split(' ', 2))
			leader_num = int(leader_num)
			seq = int(seq)
			request = self.parse_request(request_message)
			client_address = "{}:{}".format(request['host'], request['port_number'])
			if leader_num >= self.leader_num:
				if client_address not in self.accepted or self.accepted[client_address][0] < request['client_seq']:
					self.accepted[client_address] = [request['client_seq'], leader_num, seq, request['command'], 1]
				elif self.accepted[client_address][0] == request['client_seq']:
					self.accepted[client_address][4] = 1#+= 1 #??? proposed again means view change happened, recount??
				message = "Accept {} {} {}".format(str(leader_num), str(seq), request_message)
				self.broadcast(message)

		elif type_of_message == "Accept":
			# Record the # of "Accept"
			# Write it to log until it hits f+1 and remove it from accepted
			leader_num, seq, request_message = tuple(rest_of_message.split(' ', 2))
			leader_num = int(leader_num)
			seq = int(seq)
			request = self.parse_request(request_message)
			client_address = "{}:{}".format(request['host'], request['port_number'])
			if leader_num >= self.leader_num:
				if client_address not in self.accepted or self.accepted[client_address][0] < request['client_seq']:
					self.accepted[client_address] = [request['client_seq'], leader_num, seq, request['command'], 1]
				elif self.accepted[client_address][0] == request['client_seq']:
					self.accepted[client_address][4] += 1
					if self.accepted[client_address][4] >= self.max_failure + 1:
						self.append_log(seq, request['command'])
						self.execute()
						if self.isLeader():
							message = "Reply {}".format(request['client_seq'])
							self.send_message(request['host'], int(request['port_number']), message)
						del self.accepted[client_address]

		elif type_of_message == "Request":
			if self.isLeader():
				self.debug_print(' leader processing client request')
				assigned_seq = self.assigned_command_seq + 1
				self.assigned_command_seq += 1
				request = self.parse_request(message)
				client_address = "{}:{}".format(request['host'], request['port_number'])
				self.accepted[client_address] = [request['client_seq'], self.leader_num, assigned_seq, request['command'], 1]
				message = "Propose {} {} {}".format(str(self.leader_num), assigned_seq, message)
				self.broadcast(message)
			else:	# forward message to current leader
				leader_id = self.leader_num % self.num_replicas
				self.send_message(self.address_list[leader_id][0], self.address_list[leader_id][1], message)

		elif type_of_message == "WhoIsLeader":
			host, port_number = tuple(rest_of_message.split(' '))
			message = "LeaderIs {}".format(str(self.leader_num % self.num_replicas))
			self.send_message(host, port_number, message)
		

	def send_message(self, host, port_number, message):
		self.debug_print("=== sending message :"+ message + " ===")
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.connect((host, port_number))
		sock.sendall(message)
		sock.close()

	def broadcast(self, message):
		for i in xrange(self.num_replicas):
			if i != self.replica_id:
				self.send_message(self.address_list[i][0], self.address_list[i][1], message)

	# Need to update chat_log accordingly ?!
	def load_log(self):
		if os.path.isfile(self.log_filename):
			mode = "r"
			with open(self.log_filename, mode) as f:
				for line in f:
					line = line.rstrip('\n')
					key, value = tuple(line.split(' ', 1))
					self.log[key] = value

	def parse_request(self, request_message):
		components = request_message.split(' ')
		request = {
			'host': components[1],
			'port_number': components[2],
			'client_seq': components[3],
			'command': components[4]
		}
		return request

	def append_log(self, seq, command):
		with open(self.log_filename, 'a') as f:
			new_log_entry = "{} {}".format(str(seq), command)
			f.write(new_log_entry+'\n')
			self.log[seq] = command

	def execute(self):
		with open(self.chat_log_filename, 'a') as f:
			while self.executed_command_seq + 1 <= max(log.keys()):
				if self.executed_command_seq + 1 in self.log:
					f.write(self.log[self.executed_command_seq]+'\n')
				self.executed_command_seq += 1

	def debug_print(self, msg):
		print str(self.replica_id) + ': '+msg

	def isLeader(self):
		return self.leader_num % self.num_replicas == self.replica_id

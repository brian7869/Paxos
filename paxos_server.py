import socket, threading, multiprocessing, datetime, json, os
from paxos_utils import json_spaceless_dump, json_set_serializable_load
from multiprocessing import Process
from time import sleep

TIMEOUT = 10
HEARTBEART_CYCLE = 3

# make it possible to skip some sequence number
class Paxos_server(Process):
	def __init__(self, max_failure, replica_id, address_list):
		super(Paxos_server, self).__init__()
		# 0. Initialize internal data
		self.max_failure = max_failure
		self.num_replicas = 2 * max_failure + 1
		self.replica_id = replica_id
		self.address_list = address_list
		self.host = address_list[replica_id][0]
		self.port_number = address_list[replica_id][1]
		
		self.log_filename = 'log/server_{}.log'.format(str(replica_id))
		self.chat_log_filename = 'chat_log/server_{}.chat_log'.format(str(replica_id))
		self.log = {}		# {
							#	<seq>:{
							#		'client_address': <client_address>,
							#		'client_seq': <client_seq>,
							#		'command': <command>
		self.accepted = {}	# {
							#	<client_address>:{
							#		'client_seq':<client_seq>,
							#		'leader_num':<leader_num>,
							#		'seq':<seq>,
							#		'command':<command>,
							#		'accepted_replicas':<list_of_accepted_replica_id>
							#		'done': <done>
							#	}
							# }
		self.executed_command_seq = -1
		self.assigned_command_seq = -1
		self.leader_num = -1

		self.num_followers = 0

		self.replica_heartbeat = []
		
		self.load_log()
		for i in xrange(self.num_replicas):
			self.replica_heartbeat.append(datetime.datetime.now())
		
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
			if self.replica_id == new_live_set[0] and self.num_followers == 0 and \
				( (self.leader_num % self.num_replicas) not in new_live_set or self.leader_num == -1 ):
				self.num_followers = 1
				new_leader_num = self.replica_id + (self.leader_num - (self.leader_num % self.num_replicas)) 
				if new_leader_num < self.leader_num:
					new_leader_num += self.num_replicas
				message = "IAmLeader {}".format(str(new_leader_num))
				self.broadcast(message)
				self.merged_accepted = {}
			sleep(TIMEOUT)

	def message_handler(self, message):
		# Possible message
		# From replicas:
		# 0. "Heartbeat <sender_id>"
		# 1. "IAmLeader <leader_num>"
		# 2. "YouAreLeader <leader_num> <committed_log> <accepted>"
		# 3. "Propose <leader_num> <sequence_num> <Request_message>"
		# 4. "Accept <sender_id> <leader_num> <sequence_num> <Request_message>"
		# 5. "NOOP <> <seq>"
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
				response = "YouAreLeader {} {} {}".format(str(new_leader_num), json_spaceless_dump(self.log)
					, json_spaceless_dump(self.accepted))
				self.send_message(self.address_list[leader_id][0], self.address_list[leader_id][1], response)
				self.leader_num = new_leader_num
		elif type_of_message == "YouAreLeader":
			# TODO
			new_leader_num, committed_log, accepted = tuple(rest_of_message.split(' ', 2))
			if self.num_followers > 0:
				self.num_followers += 1
		 		
		 		committed_log = json.loads(committed_log)
		 		accepted = json_set_serializable_load(accepted)
		 		for seq, inner_dict in committed_log.iteritems():
		 			if seq not in self.log:
		 				self.decide_value(seq, inner_dict['client_address'], inner_dict['client_seq'], inner_dict['command'])
		 				if self.accepted[inner_dict['client_address']]['client_seq'] < inner_dict['client_seq'] and self.accepted[inner_dict['client_address']]['seq'] not in self.log:
		 					decided = self.accepted[inner_dict['client_address']]
		 					self.decide_value(decided['seq'], inner_dict['client_address'], decided['client_seq'], decided['command'])

		 		for client_address, inner_dict in accepted.iteritems():
		 			if inner_dict['seq'] not in self.merged_accepted or inner_dict['leader_num'] > self.merged_accepted[inner_dict['seq']]['leader_num']:
		 				self.merged_accepted[inner_dict['seq']] = self.get_new_merged_accepted(inner_dict['client_seq'], inner_dict['leader_num'], client_address, inner_dict['command'], inner_dict['accepted_replicas'], inner_dict['done'])
		 			elif inner_dict['leader_num'] == self.merged_accepted[inner_dict['seq']]['leader_num']:
		 				self.merged_accepted[inner_dict['seq']]['accepted_replicas'].update(inner_dict['accepted_replicas'])

		 			# if client_address not in self.accepted:
		 			# 	self.accepted[client_address] = inner_dict
		 			# elif inner_dict['client_seq'] > self.accepted[client_address]['client_seq']:
		 			# 	if self.accepted[client_address]['seq'] not in self.log:
		 			# 		decided = self.accepted[client_address]
		 			# 		self.decide_value(decided['seq'], client_address, decided['client_seq'], decided['command'])
		 			# 	self.accepted[client_address] = inner_dict
		 			# elif inner_dict['client_seq'] == self.accepted[client_address]['client_seq']:
		 			# 	self.accepted[client_address]['accepted_replicas'].update(inner_dict['accepted_replicas'])
		 			# # if # of accepted replicas exceeds f + 1
		 			# if len(self.accepted[client_address]['accepted_replicas']) >= self.max_failure + 1 and self.accepted[client_address]['seq'] not in self.log:
		 			# 	decided = self.accepted[client_address]
		 			# 	self.decide_value(decided['seq'], decided['client_address'], decided['client_seq'], decided['command'])

				if self.num_followers >= self.max_failure + 1:
					for client_address, inner_dict in self.accepted.iteritems():
			 			if inner_dict['seq'] not in self.merged_accepted or inner_dict['leader_num'] > self.merged_accepted[inner_dict['seq']]['leader_num']:
			 				self.merged_accepted[inner_dict['seq']] = self.get_new_merged_accepted(inner_dict['client_seq'], inner_dict['leader_num'], client_address, inner_dict['command'], inner_dict['accepted_replicas'], inner_dict['done'])
			 			elif inner_dict['leader_num'] == self.merged_accepted[inner_dict['seq']]['leader_num']:
			 				self.merged_accepted[inner_dict['seq']]['accepted_replicas'].update(inner_dict['accepted_replicas'])
					self.leader_num = int(new_leader_num)
					self.repropose_undecided_value()
					self.fill_holes()
					self.num_followers = 0

		elif type_of_message == "Propose":
			leader_num, seq, request_message = tuple(rest_of_message.split(' ', 2))
			leader_num = int(leader_num)
			leader_id = leader_num % self.replica_id
			seq = int(seq)
			request = self.parse_request(request_message)
			client_address = self.get_client_address(request)
			if leader_num >= self.leader_num:
				self.leader_num = leader_num
				if request['client_seq'] == -1:
					if seq not in self.accepted:
						self.accepted[seq] = self.get_new_accepted(request['client_seq'], leader_num, seq, request['command'], set([self.replica_id, leader_id]), False)
					else:
						self.accepted[seq]['accepted_replicas'].add(self.replica_id)
				elif client_address not in self.accepted:
					self.accepted[client_address] = self.get_new_accepted(request['client_seq'], leader_num, seq, request['command'], set([self.replica_id, leader_id]), False)
				elif self.accepted[client_address]['client_seq'] < request['client_seq']:
					if self.accepted[client_address]['done'] == False:
						self.decide_value(self.accepted[client_address]['seq'], self.accepted[client_address]['client_address'], self.accepted[client_address]['client_seq'], self.accepted[client_address]['command'])
					self.accepted[client_address] = self.get_new_accepted(request['client_seq'], leader_num, seq, request['command'], set([self.replica_id, leader_id]), False)
				elif self.accepted[client_address]['client_seq'] == request['client_seq']:
					self.accepted[client_address]['accepted_replicas'].add(self.replica_id)
				
				if len(self.accepted[client_address]['accepted_replicas']) >= self.max_failure + 1:
					self.append_log(seq, self.accepted[client_address]['command'])
					self.execute()
					# message = "Reply {}".format(request['client_seq'])
					# self.send_message(request['host'], request['port_number'], message)
					self.accepted[client_address]['done'] = True
				self.accept(self.replica_id, leader_num, seq, request_message)

		elif type_of_message == "Accept":
			sender_id, leader_num, seq, request_message = tuple(rest_of_message.split(' ', 3))
			leader_num = int(leader_num)
			leader_id = leader_num % self.num_replicas
			seq = int(seq)
			request = self.parse_request(request_message)
			sender_id = int(sender_id)
			client_address = self.get_client_address(request)
			if leader_num >= self.leader_num:
				self.leader_num = leader_num
				if request['client_seq'] == -1:
					if seq not in self.accepted:
						self.accepted[seq] = self.get_new_accepted(request['client_seq'], leader_num, seq, request['command'], set([self.replica_id, leader_id]), False)
					else:
						self.accepted[seq]['accepted_replicas'].add(self.replica_id)
				elif client_address not in self.accepted:
					self.accepted[client_address] = self.get_new_accepted(request['client_seq'], leader_num, seq, request['command'], set([self.replica_id, leader_id]), False)
				elif self.accepted[client_address]['client_seq'] < request['client_seq']:
					if self.accepted[client_address]['done'] == False:
						self.decide_value(self.accepted[client_address]['seq'], self.accepted[client_address]['client_address'], self.accepted[client_address]['client_seq'], self.accepted[client_address]['command'])
					self.accepted[client_address] = self.get_new_accepted(request['client_seq'], leader_num, seq, request['command'], set([self.replica_id, leader_id]), False)
				elif self.accepted[client_address]['client_seq'] == request['client_seq']:
					self.accepted[client_address]['accepted_replicas'].add(sender_id)
				if len(self.accepted[client_address]['accepted_replicas']) >= self.max_failure + 1:
					self.append_log(seq, self.accepted[client_address]['command'])
					self.execute()
					self.accepted[client_address]['done'] = True

		elif type_of_message == "Request":
			if self.isLeader():
				request = self.parse_request(message)
				client_address = self.get_client_address(request)
				# client resend the request due to timeout
				# what if the client_seq in this request is greater than self.accepted[client_address]['client_seq'] and the one in my accepted is not in the log?
				if client_address in self.accepted and self.accepted[client_address]['client_seq'] == request['client_seq']:
					if self.accepted[client_address]['seq'] in self.log:
						message = "Reply {}".format(request['client_seq'])
						self.send_message(request['host'], request['port_number'], message)
						return
					else:	# reproposed the same value
						self.propose(self.leader_num, self.accepted[client_address]['seq'], message)
				self.debug_print(' leader processing client request')
				self.assigned_command_seq += 1
				assigned_seq = self.assigned_command_seq
				self.accepted[client_address] = self.get_new_accepted(request['client_seq'], self.leader_num, assigned_seq, request['command'], set([self.replica_id]), False)
				self.propose(self.leader_num, assigned_seq, message)
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
					seq, client_address, client_seq, command = tuple(line.split(' ', 3))
					self.log[seq] = self.get_new_log(client_address, client_seq, command)

	def propose(self, leader_num, seq, request):
		propose_message = "Propose {} {} {}".format(str(leader_num), str(seq), request)
		self.broadcast(propose_message)

	def accept(self, sender_id, leader_num, seq, request):
		accept_message = "Accept {} {} {} {}".format(str(sender_id), str(leader_num), str(seq), request)
		self.broadcast(accept_message)

	def get_new_accepted(self, client_seq, leader_num, seq, command, accepted_replicas, done):
		accepted = {
			'client_seq': client_seq, 
			'leader_num': leader_num, 
			'seq': seq, 
			'command': command, 
			'accepted_replicas': accepted_replicas, 
			'done': done
		}
		return accepted

	def get_new_log(self, client_address, client_seq, command):
		log = {
			'client_address': client_address,
			'client_seq': client_seq,
			'command': command
		}
		return command

	def get_new_merged_accepted(self, client_seq, leader_num, client_address, command, accepted_replicas, done):
		merged_accepted = {
			'client_seq': client_seq, 
			'leader_num': leader_num, 
			'client_address': client_address, 
			'command': command, 
			'accepted_replicas': accepted_replicas, 
			'done': done
		}
		return merged_accepted

	def parse_request(self, request_message):
		components = request_message.split(' ')
		self.debug_print(request_message)
		request = {
			'host': components[1],
			'port_number': int(components[2]),
			'client_seq':int(components[3]),
			'command': components[4]
		}
		return request

	def decide_value(self, seq, client_address, client_seq, command):
		self.append_log(seq, client_address, client_seq, command)
		self.execute()

	def append_log(self, seq, client_address, client_seq, command):
		with open(self.log_filename, 'a') as f:
			new_log_entry = "{} {} {} {}".format(str(seq), client_address, str(client_seq), command)
			f.write(new_log_entry+'\n')
			self.log[seq] = self.get_new_log(client_address, client_seq, command)

	def execute(self):
		with open(self.chat_log_filename, 'a') as f:
			while self.executed_command_seq + 1 in self.log:
				self.executed_command_seq += 1
				log_entry = self.lof[self.executed_command_seq]
				f.write("{} {} {} {}\n".format(str(self.executed_command_seq), log_entry['client_address'], str(log_entry['client_seq']), log_entry['command']))

	def repropose_undecided_value(self):
		for seq, inner_dict in self.merged_accepted.iteritems():
			if len(inner_dict['accepted_replicas']) < self.max_failure + 1:
				host, port_number = tuple(inner_dict['client_address'].split(':', 1))
				request = "Request {} {} {} {}".format(host, port_number, inner_dict['client_seq'], inner_dict['command'])
				self.propose(self.leader_num, seq, request)

	def fill_holes(self, reproposed_seq):
		self.assigned_seq = max(max(self.log.keys()), max(reproposed_seq))
		for seq in xrange(self.assigned_seq):
			if seq not in self.log and seq not in self.merged_accepted:
				noop_request = "Request -1 -1 -1 NOOP"
				self.propose(self.leader_num, seq, noop_request)

	def debug_print(self, msg):
		print str(self.replica_id) + ': '+msg

	def isLeader(self):
		return self.leader_num % self.num_replicas == self.replica_id

	def get_client_address(self, request):
		return "{}:{}".format(request['host'], request['port_number'])
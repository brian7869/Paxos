import socket, threading, multiprocessing, datetime, json
# make it possible to skip some sequence number
class Paxo_server:
	def __init__(self, max_failure, replica_id, address_list):
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
		self.leader_num = -1
		self.leader_id = -1

		self.num_followers = 0
		self.num_accept = 0

		self.live_set = []
		self.replica_heartbeat = []
		
		load_log(self)
		for i in xrange(self.num_replicas):
			self.replica_heartbeat.append(datetime.datetime.now())

		# Locks, not sure if they are necessary... 
		self.heartbeat_lock = threading.Lock()
		self.leader_lock = threading.Lock()
		
		# 1. Create new UDP socket (receiver)
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.bind((self.host, self.port_number))

		# 2. Create heartbeat sender and failure detector
		heartbeat_sender_thread = threading.Thread(name="heartbeat_sender_" + str(replica_id),
												   target=heartbeat_sender, args=(self,))
		failure_detector_thread = threading.Thread(name="failure_detector_" + str(replica_id),
												   target=failure_detector, args=(self,))
		
		# 3. Start receiving message
		while True:
			message = sock.recv(65535)
			message_handler(self, message)

	def heartbeat_sender(self):
		heartbeat_message = "Heartbeat {}".format(str(self.replica_id))
		while True:
			for i in xrange(self.num_replicas):
				send_message(self.address_list[i][0], self.address_list[i][1], heartbeat_message)
			sleep(3)

	def failure_detector(self):
		while True:
			new_live_set = []
			for i in xrange(self.num_replicas):
				if (datetime.datetime.now() - self.replica_heartbeat[i]).total_seconds() < 10:
					new_live_set.append(i)
			self.live_set = new_live_set
			if self.replica_id == new_live_set[0] and self.leader_id != self.replica_id:
				message = "YouShouldBeLeader {}".format(str(self.replica_id))
				send_message(self.host, self.port_number, message)
			sleep(10)

	def message_handler(self, message):
		# Possible message
		# From replicas:
		# 0. "Heartbeat <sender_id>"
		# 1. "IAmLeader <leader_num>"
		# 2. "YouShouldBeLeader <sender_id>"
		# 3. "YouAreLeader <sender_id> <committed_log> <[(leader_num, seq, command)]>"
		# 4. "Propose <leader_num> <sequence_num> <Request_message>"
		# 5. "Accept <leader_num> <sequence_num> <Request_message>"
		# From clients:
		# 1. "Request <host> <port_number> <client_seq> <command>"
		# 2. "WhoIsLeader <host> <port_number>"
		type_of_message, rest_of_message = tuple(message.split(' ', 1))

		if type_of_message == "Heartbeat":
			sender_id = int(rest_of_message)
			self.replica_heartbeat[sender_id] = datetime.datetime.now()
		elif type_of_message == "IAmLeader":
			new_leader_num = int(rest_of_message)
			if new_leader_num > self.leader_num:
				leader_id = new_leader_num % self.num_replicas
				response = "YouAreLeader {} {}".format(json.dumps(self.log), json.dumps(self.accepted.values()))
				send_message(self.address_list[leader_id][0], self.address_list[leader_id][1], response)
				self.leader_num = new_leader_num
				self.leader_id = leader_id
		elif type_of_message == "YouShouldBeLeader":
			self.followers = 0
			new_leader_num = self.replica_id + (self.leader_num - self.leader_id) 
			if new_leader_num < self.leader_num:
				new_leader_num += self.num_replicas
			message = "IAmLeader {}".format(str(new_leader_num))
			for i in xrange(self.num_replicas):
				send_message(self.address_list[i][0], self.address_list[i][1], message)
		elif type_of_message == "YouAreLeader":
			# TODO

		elif type_of_message == "Propose":
			# Add it to accepted
			leader_num, seq, request_message = tuple(rest_of_message.split(' ', 2))
			request = parse_request(request_message)
			client_address = '{}:{}'.format(request['host'], request['port_number'])
			# TODO
			if leader_num >= self.leader_num:
				if client_address not in self.accepted or self.accepted[client_address][0] < request['client_seq']:
					self.accepted[client_address] = [request['client_seq'], leader_num, seq, command, 0]
				else:


		elif type_of_message == "Accept":
			# TODO
			# Record the # of "Accept"
			# Write it to log until it hits f+1 and remove it from accepted
		elif type_of_message == "Request":
			#TODO

		elif type_of_message == "WhoIsLeader":
			host, port_number = tuple(rest_of_message.split(' '))
			message = "LeaderIs {}".format(str(self.leader_id))
			send_message(host, port_number, message)
		

	def send_message(host, port_number, message):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.connect((host, port_number))
		sock.sendall(message)
		sock.close()

	# Need to update chat_log accordingly ?!
	def load_log(self):
		if os.path.isfile(self.log_filename):
			mode = "r"
			with open(self.log_filename, mode) as f:
				for line in f:
					line = line.rstrip('\n')
					key, value = tuple(line.split(' ', 1))
					self.log[key] = value

	def parse_request(request_message):
		components = request_message.split(' ')
		request = {
			'host': components[1],
			'port_number': components[2],
			'client_seq': components[3],
			'command': components[4]
		}
		return request
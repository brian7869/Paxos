from multiprocessing import Process
import socket
from time import sleep
from random import random
from config import *
import time

ACK = 1
SKIP = 2
VIEWCHG = 3
# send request to known leader, if timeout, ask all replicas WhoIsLeader and pick one with f+1
class Paxos_client(Process):
	def __init__(self, client_id, host, port, max_failure, address_list, commands):
		super(Paxos_client, self).__init__()
		self.max_failure = int(max_failure)
		self.num_replicas = 2 * max_failure + 1
		self.client_id = int(client_id)
		self.host = host
		self.port = int(port)
		self.address_list = address_list
		self.commands = commands
		self.leader = 0

	def run(self):
		# send message format	
		# "Request <host> <port_number> <client_seq> <command>"
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.bind((self.host, self.port))
		sock.settimeout(TIMEOUT)

		for client_seq in xrange(len(self.commands)):
			# sleep(random())
			self.debug_print('send request: '+self.commands[client_seq])
			req_message = "Request {} {} {} {}".format(self.host, str(self.port)
						, str(client_seq), 'client'+str(self.client_id)+':'+self.commands[client_seq])
			self.send_message(self.address_list[self.leader][0]
				, self.address_list[self.leader][1], req_message)
			while True:
				# self.debug_print('wait for message')
				try:
					start_time = time.time()
					message = sock.recv(65535)
					elapsed = time.time() - start_time
					status = self.message_handler(message, client_seq)
					if status == ACK:
						sock.settimeout(TIMEOUT)
						break
					elif status == SKIP:
						sock.settimeout(sock.gettimeout() - elapsed)
					else:
						# self.debug_print('resend request: '+self.commands[client_seq])
						self.send_message(self.address_list[self.leader][0]
							, self.address_list[self.leader][1], req_message)
						sock.settimeout(TIMEOUT)
				except socket.timeout:
					# self.debug_print('send viewchange: '+str(client_seq))
					message = "ViewChange {} {} {}".format(self.host, str(self.port), str(client_seq))
					sock.settimeout(sock.gettimeout()*2)
					self.broadcast(message)
		self.debug_print('End of request')

	def message_handler(self, message, client_seq):
		# Possible messages
		# From primary:
		# "Reply <client_seq>"
		# "LeaderIs <leader_id>"
		# return True if command succeeded
		type_of_message, rest_of_message = tuple(message.split(' ', 1))

		if type_of_message == 'Reply' and client_seq == int(rest_of_message):
			return ACK
		elif type_of_message == 'LeaderIs':
			# Modify: self.leader
			self.leader = int(rest_of_message)
			return VIEWCHG
		else:
			return SKIP


	def broadcast(self, message):
		for i in xrange(self.num_replicas):
			self.send_message(self.address_list[i][0], self.address_list[i][1], message)


	def send_message(self, host, port_number, message):
		# self.debug_print("=== sending message :"+ message + " ===")
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.connect((host, port_number))
		sock.sendall(message)
		sock.close()

	def debug_print(self, msg):
		print 'client ' + str(self.client_id) + ': '+msg

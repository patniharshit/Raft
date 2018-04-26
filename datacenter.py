import sys, time, json, threading, socket, thread,random, pickle
from multiprocessing import Process
from KThread import *
from messages import *
from follower_functions import *

class Server(object):
	def __init__(self, id_):
		self.id = id_
		self.config_file = 'config-%d' % self.id

		self.role = 'follower'
		self.commitIndex, self.lastApplied, self.leaderID = 0, 0, 0

		address = json.load(file('config.json'))
		self.initial_state = address['initial_state']
		self.addressbook = {}
		for id_ in address['running']:
			self.addressbook[id_] = address['AddressBook'][id_ - 1]

		# need to put it into file later on
		self.load()

		self.lastLogIndex = 0
		self.lastLogTerm, self.oldVotes, self.newVotes, self.numVotes = 0, 0, 0, 0

		self.port = self.addressbook[self.id]
		self.request_votes = self.peers[:]


		self.listener = KThread(target = self.listen, args= (acceptor,))
		self.listener.start()
		self.newPeers = []

	def listen(self, on_accept):
		print 'start listenning on port '+str(self.port)
		srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		srv.bind(("", self.port))
		while True:
			data, addr = srv.recvfrom(1024)
			KThread(target=on_accept, args=(self, data, addr)).start()
		srv.close()

	def follower(self):
		print 'Running as a follower'
		self.role = 'follower'
		first = True
		while True:
			self.last_update = time.time()
			election_timeout = 5 * random.random() + 5
			while time.time() - self.last_update <= election_timeout:
				pass
			if not first and self.election.is_alive():
				self.election.kill()
			first = False
			self.start_election()

	def start_election(self):
		self.role = 'candidate'
		self.election = KThread(target =self.thread_election,args = ())
		if len(self.peers) == 0:
			return
		self.currentTerm += 1
		self.votedFor = self.id
		serverConfig = ServerConfig(self.poolsize, self.currentTerm, self.votedFor, self.log, self.peers)
		with open(self.config_file, 'w') as f:
			pickle.dump(serverConfig, f)

		self.numVotes = 1
		self.election.start()

	def thread_election(self):
		print 'timouts, start a new election with term %d' % self.currentTerm
		self.role = 'candidate'
		self.request_votes = self.peers[:]

		while True:
			for peer in self.peers:
	 			if peer in self.request_votes:
	 				Msg = str(self.lastLogTerm) + ' ' + str(self.lastLogIndex)
	 				msg = BaseMessage(self.id, peer, self.currentTerm, Msg, reqtype="RequestVote")
	 				data = pickle.dumps(msg)
	 				sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	 				sock.sendto(data, ("", self.addressbook[peer]))
 			time.sleep(1) # wait for servers to receive

 	def leader(self):
 		print 'Running as a leader'
 		self.role = 'leader'
 		self.nextIndex = {}
 		self.matchIndex = {}
 		for peer in self.peers:
 			self.nextIndex[peer] = len(self.log) + 1
 			self.matchIndex[peer] = 0
		self.append_entries()

	def append_entries(self):
		receipts = self.peers[:]
		while True:
			receipts = self.peers[:]
			for peer in receipts:
				if len(self.log) < self.nextIndex[peer]:
					entries = []
					prevLogIndex = len(self.log)
					prevLogTerm = 0
					if prevLogIndex != 0:
						prevLogTerm = self.log[prevLogIndex-1].term
				else:
					prevLogIndex = self.nextIndex[peer] - 1
					prevLogTerm = 0
					if prevLogIndex != 0:
						prevLogTerm = self.log[prevLogIndex-1].term
					entries = [self.log[prevLogIndex]]


				Msg = AppendEntriesMsg(self.id, peer, self.currentTerm, entries, self.commitIndex, prevLogIndex, prevLogTerm)
				data = pickle.dumps(Msg)
				sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
				sock.sendto(data, ("", self.addressbook[peer]))
			time.sleep(0.5)

	def step_down(self):
		if self.role == 'leader':
			self.leader_state.kill()
			self.follower_state = KThread(target = self.follower, args = ())
			self.follower_state.start()
		if self.role == 'candidate':
			print 'candidate step down when higher term'
			self.election.kill()
			self.last_update = time.time()
			self.role = 'follower'

	def load(self):
		try:
			with open(self.config_file) as f:
				serverConfig = pickle.load(f)
		except Exception as e:
			initial_running = [i+1 for i in range(5)]
			initial_running.remove(self.id)
			serverConfig = ServerConfig(100, 0, -1, [], initial_running)

		self.majority = (len(self.peers) + 1)/2 + 1
		self.currentTerm = serverConfig.currentTerm
		self.poolsize = serverConfig.poolsize
		self.votedFor = serverConfig.votedFor
		self.peers = serverConfig.peers
		self.log = serverConfig.log

	def run(self):
		time.sleep(1)
		self.follower_state = KThread(target = self.follower, args = ())
		self.follower_state.start()

import sys, time, json, threading, socket, thread,random, pickle
from multiprocessing import Process
from KThread import *


class BaseMessage(object):
    def __init__(self, sender, receiver, term, data, reqtype):
        self.sender = sender
        self.receiver = receiver
        self.term = term
        self.data = data
        if(reqtype == "RequestVote"):
            self.type = 1
        elif(reqtype == "RequestVoteResponse"):
            self.type = 2
        elif(reqtype == "AppendEntries"):
            self.type = 0
        elif(reqtype == "AppendEntriesResponse"):
            self.type = 3

class AppendEntriesMsg(BaseMessage):
    def __init__(self, sender, receiver, term, entries, commitIndex, prevLogIndex, prevLogTerm):
        BaseMessage.__init__(self, sender, receiver, term, data=None, reqtype="AppendEntries")
        self.entries = entries
        self.commitIndex = commitIndex
        self.prevLogTerm = prevLogTerm
        self.prevLogIndex = prevLogIndex

class AppendEntriesResponseMsg(BaseMessage):
    def __init__(self, sender, receiver, term, success, matchIndex):
        BaseMessage.__init__(self, sender, receiver, term, data=None, reqtype="AppendEntriesResponse")
        self.success = success
        self.matchIndex = matchIndex

class LogEntry(object):
    def __init__(self, term, command, addr, uuid, _type = 0):
        self.term = term
        self.command = command
        self.uuid = uuid
        self.addr = addr
        self.type = _type

class Request(object):
    def __init__(self, request_msg, uuid = 0, addr=None, reqtype='client'):
        self.request_msg = request_msg
        self.type = reqtype
        self.uuid = uuid

class RequestRedirect(Request):
    def __init__(self, request_msg, uuid, addr, reqtype='redirect'):
        Request.__init__(self, request_msg, uuid, addr, reqtype='redirect')
        self.addr = addr

class ServerConfig(object):
    def __init__(self, poolsize, currentTerm, votedFor, log, peers):
        self.poolsize = poolsize
        self.currentTerm = currentTerm
        self.votedFor = votedFor
        self.log = log
        self.peers = peers


def show_command(server,Msg,addr):
	tickect_remaining = server.poolsize

	all_log = ''
	for entry in server.log:
		all_log += str(entry.command) + ' '

	committed_log = ''
	for idx in range(0,server.commitIndex):
		entry = server.log[idx]
		committed_log += str(entry.command) + ' '


	show_msg = 'state machine: ' + str(tickect_remaining) + '\n' + 'committed log: ' + committed_log + '\n' + 'all log:' + all_log
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.sendto(show_msg, addr)
	s.close()
	return

def buy_ticket(server,Msg,addr):
	msg_string = Msg.request_msg
	msg_data = msg_string.split()
	ticket_num = int(msg_data[1])
	print "I am the leader, customer wants to buy ",ticket_num, " tickets"

	if ticket_num > server.poolsize:
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		print 'Tickets not enough'
		msgPass = 'We do not have enough tickets'
		s.sendto(msgPass, addr)
		s.close()
		return 0

	# check whether this command has already been
	idx = 0
	for entry in server.log:
		idx += 1
		if entry.uuid == Msg.uuid:
			if server.commitIndex >= idx:
				s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				msgPass = 'Your request has been fullfilled'
				s.sendto(msgPass, addr)
				s.close()
			else: # ignore
				pass
			return 0 # ignore this new command

	newEntry = LogEntry(server.currentTerm, ticket_num, addr, Msg.uuid)
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	msgPass = 'The Leader gets your request'
	s.sendto(msgPass, addr)
	s.close()
	server.log.append(newEntry)
	serverConfig = ServerConfig(server.poolsize, server.currentTerm, server.votedFor, server.log, server.peers)
	with open(server.config_file, 'w') as f:
		pickle.dump(serverConfig, f)

	return 1

def fun_redirect(server,Msg,addr):
	msg_string = Msg.request_msg
	print 'redirect the request to leader'
	if server.leaderID == 0:
		target_redirect = random.choice(server.peers)
	else:
		target_redirect = server.leaderID
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	msg_redirect = RequestRedirect(msg_string, Msg.uuid, addr)
	s.sendto(pickle.dumps(msg_redirect), ("",server.addressbook[target_redirect]))
	s.close()
	return

def vote_response(server,Msg):
	_sender = Msg.sender
	_term = Msg.term
	_msg = Msg.data
	_msg = _msg.split()
	logTerm = int(_msg[0])
	logIndex = int(_msg[1])
	log_info = (logTerm, logIndex)
	print '---------Get requestvote message---------'

	if _term == server.currentTerm:
		if (server.votedFor == _sender or server.votedFor == -1) and log_info >= (server.lastLogTerm, server.lastLogIndex) :
			server.votedFor = _sender
			voteGranted = 1
			serverConfig = ServerConfig(server.poolsize, server.currentTerm, server.votedFor, server.log, server.peers)
			with open(server.config_file, 'w') as f:
				pickle.dump(serverConfig, f)
		else:
			voteGranted = 0

	elif _term < server.currentTerm:
		voteGranted = 0
		print 'rejected due to old term'

	else:
		# find higher term in RequestVoteMsg
		server.currentTerm = _term
		serverConfig = ServerConfig(server.poolsize, server.currentTerm, server.votedFor, server.log, server.peers)
		with open(server.config_file, 'w') as f:
			pickle.dump(serverConfig, f)
		server.step_down()
		if log_info < (server.lastLogTerm, server.lastLogIndex):
			voteGranted = 0
		else :
			server.votedFor = _sender
			voteGranted = 1
			serverConfig = ServerConfig(server.poolsize, server.currentTerm, server.votedFor, server.log, server.peers)
			with open(server.config_file, 'w') as f:
				pickle.dump(serverConfig, f)

	reply = str(voteGranted)
	reply_msg = BaseMessage(server.id, _sender, server.currentTerm, reply, reqtype="RequestVoteResponse")
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.sendto(pickle.dumps(reply_msg), ("",server.addressbook[_sender]))

	return


def acceptor(server, data, addr):
	Msg = pickle.loads(data)
	_type = Msg.type

	# deal with client's message
	if _type == 'client' or _type == 'redirect':
		msg_string = Msg.request_msg
		if _type == 'redirect':
			addr = Msg.addr
		if msg_string == 'show':
			show_command(server,Msg,addr)

		else:
			if server.role == 'leader':
				if buy_ticket(server,Msg,addr) == 0:
					return
			# we need to redirect the request to leader
			else:
				fun_redirect(server,Msg,addr)
		return

	_sender = Msg.sender
	_term = Msg.term

	if _type == 1: # requestvote message
		if _sender not in server.peers:
			return
		vote_response(server,Msg)

	elif _type == 2: # Vote response message
		voteGranted = int(Msg.data)
		print '---------Get vote response message---------'
		if voteGranted:
			if server.role == 'candidate':
				server.numVotes += 1
				server.request_votes.remove(_sender)
				if server.numVotes == server.majority:
					print 'Get majority votes, become leader at Term ', server.currentTerm
					if server.election.is_alive():
						server.election.kill()
					# becomes a leader
					server.follower_state.kill()
					server.role = 'leader'
					server.leader_state = KThread(target = server.leader, args = ())
					server.leader_state.start()


		else:
			print 'vote rejected by ', _sender
			if server.currentTerm < _term: # discover higher term
				server.currentTerm = _term
				serverConfig = ServerConfig(server.poolsize, server.currentTerm, server.votedFor, server.log, server.peers)
				with open(server.config_file, 'w') as f:
					pickle.dump(serverConfig, f)
				if server.role == 'candidate':
					server.step_down()


	elif _type == 0: # AppendEntries msg
		"""---->>> Done till here <<<---"""
		# print '---------Get AppendEntries message---------'
		prevLogTerm = Msg.prevLogTerm
		leaderCommit = Msg.commitIndex
		matchIndex = server.commitIndex
		prevLogIndex = Msg.prevLogIndex
		entries = Msg.entries


		# This is a valid new leader
		if server.currentTerm <= _term:
			server.currentTerm = _term
			serverConfig = ServerConfig(server.poolsize, server.currentTerm, server.votedFor, server.log, server.peers)
			with open(server.config_file, 'w') as f:
				pickle.dump(serverConfig, f)
			server.step_down()
			if server.role == 'follower':
				server.last_update = time.time()
			if prevLogIndex != 0:
				if len(server.log) >= prevLogIndex:
					if server.log[prevLogIndex - 1].term == prevLogTerm:
						success = 'True'
						server.leaderID = _sender
						if len(entries) != 0:
							server.log = server.log[:prevLogIndex] + entries
							matchIndex = len(server.log)
							if entries[0].type == 1:
								server.new = entries[0].command.new_config[:]
								server.old = server.peers[:]
								server.old.append(server.id)
								server.peers = list(set(server.old + server.new))
								server.peers.remove(server.id)
							elif entries[0].type == 2:
								server.new = entries[0].command.new_config[:]
								server.peers = server.new[:]
								server.peers.remove(server.id)
								print 'follower applied new config, running peers', server.peers
								serverConfig = ServerConfig(server.poolsize, server.currentTerm, server.votedFor, server.log, server.peers)
								with open(server.config_file, 'w') as f:
									pickle.dump(serverConfig, f)
					else:
						success = 'False'
				else:
					success = 'False'
			else:
				success = 'True'
				if len(entries) != 0:
					server.log = server.log[:prevLogIndex] + entries
					if entries[0].type == 1:
						server.new = entries[0].command.new_config[:]
						server.old = server.peers[:]
						server.old.append(server.id)
						server.peers = list(set(server.old + server.new))
						server.peers.remove(server.id)
					elif entries[0].type == 2:
						server.new = entries[0].command.new_config[:]
						server.peers = server.new[:]
						server.peers.remove(server.id)
						print 'follower applied new config, running peers', server.peers

					serverConfig = ServerConfig(server.poolsize, server.currentTerm, server.votedFor, server.log, server.peers)
					with open(server.config_file, 'w') as f:
						pickle.dump(serverConfig, f)
					matchIndex = len(server.log)
				server.leaderID = _sender
		else:
			success = 'False'

		if leaderCommit > server.commitIndex:
			lastApplied = server.commitIndex
			server.commitIndex = min(leaderCommit, len(server.log))
			if server.commitIndex > lastApplied:
				server.poolsize = server.initial_state
				for idx in range(1, server.commitIndex + 1):
					if server.log[idx-1].type == 0:
						server.poolsize -= server.log[idx - 1].command

		reply_msg = AppendEntriesResponseMsg(server.id, _sender, server.currentTerm, success, matchIndex)
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		s.sendto(pickle.dumps(reply_msg), ("",server.addressbook[_sender]))

	elif _type == 3: # AppendEntriesResponse:
		#print '---------Get AppendEntries Response message---------'
		success = Msg.success
		matchIndex = Msg.matchIndex

		if success == 'False':
			if _term > server.currentTerm:
				server.currentTerm = _term
				serverConfig = ServerConfig(server.poolsize, server.currentTerm, server.votedFor, server.log, server.peers)
				with open(server.config_file, 'w') as f:
					pickle.dump(serverConfig, f)
				server.step_down()
			else:
				server.nextIndex[_sender] -= 1
		else:
			if server.nextIndex[_sender] <= len(server.log) and matchIndex > server.matchIndex[_sender]:
				server.matchIndex[_sender] = matchIndex
				server.nextIndex[_sender] += 1

			if server.commitIndex < max(server.matchIndex.values()):
				start = server.commitIndex + 1
				for N in range(start,max(server.matchIndex.values()) + 1):
					# not in config change
					compare = 1
					for key, item in server.matchIndex.items():
						if key in server.peers and item >= N:
							compare += 1
					majority = (len(server.peers) + 1)/2 + 1
					if compare == server.majority and server.log[N-1].term == server.currentTerm:
						for idx in range(server.commitIndex + 1, N + 1):
							server.poolsize -= server.log[idx-1].command
							serverConfig = ServerConfig(server.poolsize, server.currentTerm, server.votedFor, server.log, server.peers)
							with open(server.config_file, 'w') as f:
								pickle.dump(serverConfig, f)
							s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
							s.sendto('Your request is fullfilled',server.log[idx-1].addr)
							s.close()
							print 'reply once'
						server.commitIndex = N


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

		self.currentTerm = serverConfig.currentTerm
		self.poolsize = serverConfig.poolsize
		self.votedFor = serverConfig.votedFor
		self.peers = serverConfig.peers
		self.majority = (len(self.peers) + 1)/2 + 1
		self.log = serverConfig.log

	def run(self):
		time.sleep(1)
		self.follower_state = KThread(target = self.follower, args = ())
		self.follower_state.start()

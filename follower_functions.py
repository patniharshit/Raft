import socket
import pickle
import time
import random

from messages import *
from KThread import *

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
	server.save()

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
			server.save()
		else:
			voteGranted = 0

	elif _term < server.currentTerm:
		voteGranted = 0
		print 'rejected due to old term'

	else:
		# find higher term in RequestVoteMsg
		server.currentTerm = _term
		server.save()
		server.step_down()
		if log_info < (server.lastLogTerm, server.lastLogIndex):
			voteGranted = 0
		else :
			server.votedFor = _sender
			voteGranted = 1
			server.save()

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
				server.save()
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
			server.save()
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
								server.during_change = 1
								server.new = entries[0].command.new_config[:]
								server.old = server.peers[:]
								server.old.append(server.id)
								server.peers = list(set(server.old + server.new))
								server.peers.remove(server.id)
							elif entries[0].type == 2:
								server.during_change = 2
								server.new = entries[0].command.new_config[:]
								server.peers = server.new[:]
								server.peers.remove(server.id)
								print 'follower applied new config, running peers', server.peers
								server.save()
					else:
						success = 'False'
				else:
					success = 'False'
			else:
				success = 'True'
				if len(entries) != 0:
					server.log = server.log[:prevLogIndex] + entries
					if entries[0].type == 1:
						server.during_change = 1
						server.new = entries[0].command.new_config[:]
						server.old = server.peers[:]
						server.old.append(server.id)
						server.peers = list(set(server.old + server.new))
						server.peers.remove(server.id)
					elif entries[0].type == 2:
						server.during_change = 2
						server.new = entries[0].command.new_config[:]
						server.peers = server.new[:]
						server.peers.remove(server.id)
						print 'follower applied new config, running peers', server.peers

					server.save()
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
					elif server.log[idx-1].type == 2:
						server.during_change = 0

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
				server.save()
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
					if server.during_change == 0:
						# not in config change
						compare = 1
						for key, item in server.matchIndex.items():
							if key in server.peers and item >= N:
								compare += 1
						majority = (len(server.peers) + 1)/2 + 1
						if compare == server.majority and server.log[N-1].term == server.currentTerm:
							for idx in range(server.commitIndex + 1, N + 1):
								server.poolsize -= server.log[idx-1].command
								server.save()
								s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
								s.sendto('Your request is fullfilled',server.log[idx-1].addr)
								s.close()
								print 'reply once'
							server.commitIndex = N
					elif server.during_change == 1:
						majority_1 = len(server.old)/2 + 1
						majority_2 = len(server.new)/2 + 1
						votes_1 = 0
						votes_2 = 0
						if server.id in server.old:
							votes_1 = 1
						if server.id in server.new:
							votes_2 = 1
						for key, item in server.matchIndex.items():
							if item >= N:
								if key in server.old:
									votes_1 += 1
								if key in server.new:
									votes_2 += 1
						if votes_1 >= majority_1 and votes_2 >= majority_2 and server.log[N-1].term == server.currentTerm:
							server.commitIndex = N
							poolsize = server.initial_state
							for idx in range(1, N + 1):
								if server.log[idx-1].type == 0:
									poolsize -= server.log[idx-1].command
							server.poolsize = poolsize
							server.save()
							s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
							s.sendto('Your request is fullfilled',server.log[idx-1].addr)
							s.close()
							# print 'send old_new once'

					else:
						majority = len(server.new)/2 + 1
						votes = 0
						if server.id in server.new:
							votes = 1
						for key, item in server.matchIndex.items():
							if item >= N:
								if key in server.new:
									votes += 1
						if votes == majority and server.log[N-1].term == server.currentTerm:
							print '----------here 2----------'
							for idx in range(server.commitIndex + 1, N + 1):
								if server.log[idx-1].type == 0:
									server.poolsize -= server.log[idx-1].command
									server.save()
									s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
									s.sendto('Your request is fullfilled',server.log[idx-1].addr)
									s.close()
									server.commitIndex = idx
								elif server.log[idx-1].type == 2:
									server.commitIndex = idx
									time.sleep(1)
									if not server.id in server.new:
										print 'I am not in the new configuration'
										server.step_down()
									server.during_change = 0
									server.save()
									s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
									s.sendto('Your request is fullfilled',server.log[idx-1].addr)
									s.close()
								# print 'send new once'

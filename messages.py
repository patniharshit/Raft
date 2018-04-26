import time
import socket

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

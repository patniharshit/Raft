import time
import socket

class BaseMessage(object):
    AppendEntries = 0
    RequestVote = 1
    RequestVoteResponse = 2
    AppendEntriesResponse = 3

    def __init__(self, sender, receiver, term):
        self.sender = sender
        self.receiver = receiver
        self.term = term


class RequestVoteMsg(BaseMessage):
    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term)
        self.data = data
        self.type = BaseMessage.RequestVote


class VoteResponseMsg(BaseMessage):
    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term)
        self.type = BaseMessage.RequestVoteResponse
        self.data = data

class AppendEntriesMsg(BaseMessage):
    def __init__(self, sender, receiver, term, entries, commitIndex, prevLogIndex, prevLogTerm):
        BaseMessage.__init__(self, sender, receiver, term)
        self.type = BaseMessage.AppendEntries
        self.entries = entries
        self.commitIndex = commitIndex
        self.prevLogTerm = prevLogTerm
        self.prevLogIndex = prevLogIndex

class AppendEntriesResponseMsg(BaseMessage):
    def __init__(self, sender, receiver, term, success, matchIndex):
        BaseMessage.__init__(self, sender, receiver, term)
        self.success = success
        self.type = BaseMessage.AppendEntriesResponse
        self.matchIndex = matchIndex

class LogEntry(object):
    def __init__(self, term, command, addr, uuid, _type = 0):
        self.term = term
        self.command = command
        self.uuid = uuid
        self.addr = addr
        self.type = _type

class Request(object):
    """docstring for Request"""
    def __init__(self, request_msg, uuid = 0, reqtype='client'):
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

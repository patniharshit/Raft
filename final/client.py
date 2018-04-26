import socket
import json
import pickle
import uuid
import time

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


class client(object):
    cnt = 0
    def __init__(self):
        client.cnt = client.cnt+1
        self.id = client.cnt
        self.num_of_reply = 0

    def buyTickets(self, port, buy_msg, uuid):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = Request(buy_msg, uuid)
        s.sendto(pickle.dumps(msg), ("", port))
        while 1:
            try:
                reply, addr = s.recvfrom(1024)
                if reply != '':
                    self.num_of_reply += 1
                    print(reply)
                if self.num_of_reply == 2:
                    break
            except Exception as e:
                print 'Connection refused'

        s.close()

    def show_state(self, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = Request('show')
        s.sendto(pickle.dumps(msg),("",port))
        while 1:
            try:
                reply, addr = s.recvfrom(1024)
                if reply != '':
                    print 'Pool Size', reply
                    break
            except Exception as e:
                print 'Connection refused'


def main():
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        ports = config['AddressBook']
        num_ports = len(ports)
    except Exception as e:
        raise e

    while True:
        customer = client()
        server_id = input('Whom to connect ? 1-%d: ' % num_ports )
        request = raw_input('How can we help you? --')
        if request == 'buy':
            uuid_ = uuid.uuid1()
            requestThread = KThread(target = customer.buyTickets, args =  (ports[server_id - 1], request, uuid_))
            timeout = 5
        else:
            requestThread = KThread(target = customer.show_state, args = (ports[server_id - 1],))
            timeout = 5
        start_time = time.time()
        requestThread.start()
        while time.time() - start_time < timeout:
            if not requestThread.is_alive():
                break
        if requestThread.is_alive():
            requestThread.kill()
            msgP = 'Timeout! Try again'
            print msgP

if __name__ == '__main__':
    main()

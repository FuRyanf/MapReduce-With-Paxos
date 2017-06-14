#!/usr/bin/env python
import socket
import argparse
import threading
import time
import os
from copy import copy
import ast
from collections import defaultdict

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("setup_file", type=str, help="configure with setup file")
    parser.add_argument("-d", "--debug", help="display debug messages", action="store_true")
    parser.add_argument("-s", "--str_log", help="replicate with strings", action="store_true")
    args = parser.parse_args()
    prm = PRM(args.setup_file, args.debug, args.str_log)
    prm.connecting()

## Paxos Replication Module
class PRM(object):
    def __init__(self, setup_file, debug=True, str_log=False):
        self.debug = debug
        self.str_log = str_log
        self.socket_dict = {}
        self.site_to_connection = {}
        ## Parse with setupfile
        with open(setup_file, 'r') as f:
            try:
                ## Parse site_id and num_sites
                line = f.readline().split(None,2)
                self.site_id = int(line[0])
                self.num_sites = int(line[1])
                ## Parse CLI connection
                ip_addr, port = f.readline().split()[:2]
                self.site_to_connection["cli"] = (ip_addr, int(port))
                ## Parse PRM connections
                for i in range(1,4):
                    ip_addr, port = f.readline().split()[:2]
                    if i == self.site_id:
                        self.site_to_connection["prm" + str(i)] = ("0.0.0.0", int(port))
                    else:
                        self.site_to_connection["prm" + str(i)] = (ip_addr, int(port))
            except:
                print("ERROR: Can't read setup file\n")
                os._exit(1)
        self.outgoingSites = [x for x in range(1, self.num_sites+1) if x != self.site_id]
        self.ballotNum = Ballot(0, self.site_id)
        self.acceptNum = Ballot(0, self.site_id)
        self.acceptVal = None
        self.initialVal = None
        self.ackMajorityCount = 1 # include yourself
        self.acceptMajorityCount = 1 # include yourself
        self.storeAck = []
        self.sendAcceptsToAll = True # First time only
        self.log = []
        self.not_decided = True
        self.active = True

        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.start()

    def start_server(self):
        BUFFER_SIZE = 1024
        # BUFFER_SIZE = 131072
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(self.site_to_connection["prm" + str(self.site_id)])
        sock.listen(3)
        incoming_list = []
        read_buffer = ["","",""]
        while len(incoming_list) != 3:
            conn, addr = sock.accept()
            conn.setblocking(0)
            incoming_list.append(conn)
        print("Connections established.\n")
        while True:
            for index, inc in enumerate(incoming_list):
                try: #
                    data = inc.recv(BUFFER_SIZE)
                except:
                    continue
                if not data: break

                # Add data to the buffer.
                read_buffer[index] += data
                parse = read_buffer[index].split("~")
                latest_command = parse.pop() # Only the latest command can be "incomplete"

                for data in parse:
                    if not data: break
                    if self.debug == True:
                        print("  Received: {0}".format(data)) # Alex Wu: Debugging purposes
                    split = data.split()

                    ## Stopped process recived an updates after it sent a replicate
                    if split[0] == "UPDATE_LOG":
                        new_log_index = int(split[1])
                        if self.str_log:
                            new_log_entry = split[2]
                        else:
                            filename = split[2]
                            wordcount_str = split[3]
                            new_log_entry = LogEntry(filename, wordcount_str)
                        self.handle_update_log(new_log_index, new_log_entry)

                    ## CLI Commands.
                    if split[0] == "RESUME":
                        self.handle_resume()
                    if not self.active:
                        print("Inactive.")
                        break
                    if split[0] == "REPLICATE":
                        filename = split[1]
                        if self.str_log == True:
                            self.initialVal = filename
                        else:
                            wordcount_str = "".join(split[2:])
                            self.initialVal = LogEntry(filename, wordcount_str)
                        print("calls prepare")
                        self.send_prepare()
                    if split[0] == "STOP":
                        self.handle_stop()
                    if split[0] == "TOTAL":
                        try:
                            positions = [int(x) for x in split[1:]]
                        except:
                            print("Invalid position arguments for TOTAL command")
                            positions = []
                        self.handle_total(positions)
                    if split[0] == "PRINT":
                        self.handle_print()
                    if split[0] == "MERGE":
                        pos1 = int(split[1])
                        pos2 = int(split[2])
                        self.handle_merge(pos1, pos2) # TODO

                    ## Paxos protocol.
                    if split[0] == "PREPARE":
                        log_index = int(split[1])
                        bal = Ballot(split[2],split[3])
                        self.handle_prepare(log_index, bal)
                    if split[0] == "ACK":
                        log_index = int(split[1])
                        bal = Ballot(split[2],split[3])
                        acceptNum = Ballot(split[4],split[5])
                        if self.str_log == True:
                            acceptVal = split[6]
                        elif split[6] != "None":
                            filename = split[6]
                            wordcount_str = split[7]
                            acceptVal = LogEntry(filename, wordcount_str)
                        else:
                            acceptVal = None
                        self.handle_ack(log_index, bal, acceptNum, acceptVal)
                    if split[0] == "ACCEPT":
                        log_index = int(split[1])
                        b = Ballot(split[2],split[3])
                        if self.str_log == True:
                            val = split[4]
                        elif split[4] != "None":
                            filename = split[4]
                            wordcount_str = split[5]
                            val = LogEntry(filename, wordcount_str)
                        else:
                            val = None
                        self.handle_accept(log_index, b, val)
                read_buffer[index] = latest_command
                inc.send(data)
            #conn.close()

    ##------------------##
    ## Helper functions ##
    ##------------------##

    def message(self, outgoingSite, message):
        if self.debug:
            print("Sent {0}: {1}".format(outgoingSite, message)) # Alex Wu: Debugging purposes
        print(self.socket_dict)
        self.socket_dict[outgoingSite].send(message + "~")

    def connecting(self):
        RETRY_TIME = 2
        ## Connect to other PRMs
        for site_id in self.outgoingSites:
            while True:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(self.site_to_connection["prm" + str(site_id)])
                    self.socket_dict[site_id] = sock
                    break
                except:
                    if self.debug:
                        print("Attempt to connect to " + str(self.site_to_connection["prm" + str(site_id)]) +
                          " failed. Retrying in " + str(RETRY_TIME) + " seconds.")
                    time.sleep(RETRY_TIME)
        ## Connect to CLI
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(self.site_to_connection["cli"])
                self.socket_dict["cli"] = sock
                break
            except:
                if self.debug:
                    print("Attempt to connect to " + str(self.site_to_connection["cli"]) +
                      " failed. Retrying in " + str(RETRY_TIME) + " seconds.")
                time.sleep(RETRY_TIME)


    def new_round(self):
        if self.debug:
            print("  new_round()")
        self.ballotNum = Ballot(0, self.site_id)
        self.acceptNum = Ballot(0, self.site_id)
        self.acceptVal = None
        self.initialVal = None
        self.ackMajorityCount = 1 # include yourself
        self.acceptMajorityCount = 1 # include yourself
        self.storeAck = []
        self.sendAcceptsToAll = True # First time only
        self.not_decided = True

    def write_file(self, log_entry):
        written = str(log_entry.wordcount)
        f = open(log_entry.filename + str(self.site_id), 'w')
        f.write(written)  # python will convert \n to os.linesep
        f.close()

    ##-----------------------##
    ## Handling CLI commands ##
    ##-----------------------##

    def handle_stop(self):
        if self.active:
            self.active = False
            print("Site {0} deactivated.".format(self.site_id))
        else:
            print("Site {0} already unactive.".format(self.site_id))

    def handle_resume(self):
        if self.active:
            print("Site {0} already active.".format(self.site_id))
        else:
            self.active = True
            print("Site {0} activated.".format(self.site_id))

    def handle_total(self, positions):
        total = 0
        for position in positions:
            if position >= 0 and position < len(self.log):
                total += sum([count for count in self.log[position].wordcount.values()])
            # elif self.debug == True:
            else:
                print("Invald position: {0}".format(position))
        print("TOTAL: {0}".format(total))

    def handle_print(self):
        if self.str_log:
            print(self.log)
        else:
            print("Filenames:")
            filenames = [l.filename for l in self.log]
            for log_index, filename in enumerate(filenames):
                print("    {0}: {1}".format(log_index, filename))

    def handle_merge(self, pos1, pos2):
        if pos1 < 0 or pos2 < 0 or pos1 >= len(self.log) or pos2 >= len(self.log):
            print("Invalid merge positions: {0}, {1}".format(pos1, pos2))
            return
        dicts = [self.log[pos1].wordcount, self.log[pos2].wordcount]
        ret = defaultdict(int)
        for d in dicts:
            for k, v in d.items():
                ret[k] += v
        printed_dict = dict(ret)
        for key, val in printed_dict.iteritems():
            print("{0} : {1}".format(key,val))


    ##-------------------------##
    ## Handling Paxos protocol ##
    ##-------------------------##

    def send_prepare(self):
        print("in send prepare")
        # self.ballotNum.myId = self.site_id
        self.ballotNum.increment() # ballotNum is now (num+1, myId)
        self.not_decided = True
        print self.outgoingSites
        for site in self.outgoingSites:
            if site == "cli":
                continue
            self.message(site, "PREPARE {0} {1}".format(len(self.log), self.ballotNum))

    def handle_prepare(self, log_index, bal):
        ## Request updates
        if log_index > len(self.log):
            if self.debug:
                print("  handle_prepare: requesting updates from {0}".format(bal.myId))
            self.message(bal.myId, "PREPARE {0} {1}".format(len(self.log), self.ballotNum))
            return
        ## Send updates to node we received messages from since their log is behind ours
        if log_index < len(self.log):
            if self.debug:
                print("  handle_prepare: sending updates to {0}".format(bal.myId))
            for new_log_index in range(log_index, len(self.log)):
                self.message(bal.myId, "UPDATE_LOG {0} {1}".format(new_log_index, self.log[new_log_index]))
            return
        if bal >= self.ballotNum:
            self.ballotNum = copy(bal)
            self.message(bal.myId, "ACK {0} {1} {2} {3}".format(log_index, bal, self.acceptNum, self.acceptVal))

    def handle_ack(self, log_index, bal, acceptNum, acceptVal):
        if log_index != len(self.log):
            return
        self.ackMajorityCount += 1
        if acceptVal != None:
            self.storeAck.append((bal, acceptNum, acceptVal))
        ## Once we get a majority of acknowledgements, we sends accpets to everyone (first time only)
        if self.ackMajorityCount > self.num_sites/2:
            ballot = self.ballotNum
            if len(self.storeAck) == 0:
                self.acceptVal = self.initialVal
            else:
                self.acceptVal = max(self.acceptVal, key=lambda ack:ack[1])[2]
                if self.debug:
                    print("  handle_ack, acceptVal", self.acceptVal)
            if self.sendAcceptsToAll:
                for site in self.outgoingSites:
                    if site == "cli":
                        continue
                    self.message(site, "ACCEPT {0} {1} {2}".format(log_index, self.ballotNum, self.acceptVal))
                self.sendAcceptsToAll = False

    def handle_accept(self, log_index, bal, val):
        if log_index > len(self.log):
            return
        if log_index < len(self.log):
            # print("  handle_accept: ignore [{0}], val: {1}, log_index: {2}".format(bal, val, log_index)) # Alex debugging
            return
        self.acceptMajorityCount += 1
        if self.debug:
            print("  handle_accept: bal [{0}],  self.ballotNum [{1}], sendAcceptsToAll: {2}".format(bal, self.ballotNum, self.sendAcceptsToAll))
        if bal >= self.ballotNum:
            self.acceptNum = copy(bal)
            self.acceptVal = val
            if self.sendAcceptsToAll:
                for site in self.outgoingSites:
                    if site == "cli":
                        continue
                    self.message(site, "ACCEPT {0} {1} {2}".format(log_index, self.ballotNum, self.acceptVal))
                self.sendAcceptsToAll = False

        if self.acceptMajorityCount > self.num_sites/2 and self.not_decided:
            self.acceptVal = val
            if self.sendAcceptsToAll:
                for site in self.outgoingSites:
                    if site == "cli":
                        continue
                    self.message(site, "ACCEPT {0} {1} {2}".format(log_index, self.ballotNum, self.acceptVal))
                self.sendAcceptsToAll = False
            self.not_decided = False
            if self.debug:
                print("  Decided: {0}".format(self.acceptVal))
                print("----------------------------------")
            self.log.append(self.acceptVal)
            self.write_file(self.acceptVal)
            self.message("cli", "DECIDE {0}".format(self.acceptVal))
            self.new_round()
            return

    def handle_update_log(self, new_log_index, new_log_entry):
        if new_log_index == len(self.log):
            print("  Updating log position: {0}".format(len(self.log)))
            self.log.append(new_log_entry)

class Ballot(object):
    def __init__(self, num=0, myId=0):
        self.num = int(num)
        self.myId = int(myId)
    def __str__(self):
        return "{0} {1}".format(self.num, self.myId)
    def __lt__(self,other):
        if int(self.num) < int(other.num):
            return True
        return int(self.myId) < int(other.myId)
    def __ge__(self,other):
        if int(self.num) >= int(other.num):
            return True
        return int(self.myId) >= int(other.myId)
    def __eq__(self,other):
        return self.num == other.num and self.myId == other.myId
    def increment(self):
        self.num += 1

class LogEntry(object):
    def __init__(self, filename, wordcount_str=""):
        self.filename = filename
        try:
            self.wordcount = self.str_to_dict(wordcount_str)
        except:
            self.wordcount = {}
    def __str__(self):
        return "{0} {1}".format(self.filename, "".join(str(self.wordcount).split()))
    def str_to_dict(self, str):
        return ast.literal_eval(str)


if __name__ == "__main__":
    main()

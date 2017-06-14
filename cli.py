#! /usr/bin/python
import cmd
import socket
import time
import Queue
# import sys
import threading
import argparse
import os # for os._exit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("setup_file", type=str, help="configure with setup file")
    parser.add_argument("-d", "--debug", help="display debug messages", action="store_true")
    parser.add_argument("-s", "--str_log", help="replicate with strings", action="store_true")
    args = parser.parse_args()
    # cli = MapReduceReplicate(args.setup_file, args.debug, args.str_log)
    cli = MapReduceReplicate(args.setup_file)
    cli.connecting()
    cli.cmdloop()
    # MapReduceReplicate().cmdloop()
    # if len(sys.argv) > 1:
    #     MapReduceReplicate().onecmd(' '.join(sys.argv[1:]))
    # else:
    #     MapReduceReplicate().cmdloop()

class MapReduceReplicate(cmd.Cmd):
    """Client API for Map-Reduce Replicate"""
    def __init__(self, setup_file, debug=False, str_log=False):
        cmd.Cmd.__init__(self)
        self.debug = debug
        self.str_log = str_log
        self.intro = "MapReduceReplicate 1.0.0 by Alexander Wu and Ryan Fu\nType 'help' for more information."
        self.prompt = ">>> "
        self.socket_dict = {}
        self.site_to_connection = {}
        self.q = Queue.Queue(maxsize=0)
        ## Parse setup file
        with open(setup_file) as f:
            try:
                ## Parse site_id
                self.site_id = int(f.readline().split()[0])
                ## Parse CLI connection
                ip_addr, port = f.readline().split()[:2]
                self.site_to_connection["cli"] = ("0.0.0.0", int(port))
                ## Parse Mappers
                ip_addr, port = f.readline().split()[:2]
                self.site_to_connection["map_a"] = (ip_addr, int(port))
                ip_addr, port = f.readline().split()[:2]
                self.site_to_connection["map_b"] = (ip_addr, int(port))
                ## Parse Reducer
                ip_addr, port = f.readline().split()[:2]
                self.site_to_connection["reduce"] = (ip_addr, int(port))
                ## Parse PRM connection
                ip_addr, port = f.readline().split()[:2]
                self.site_to_connection["prm"] = (ip_addr, int(port))
            except: #ValueError
                print("ERROR: Can't read setup file\n")
                os._exit(1)
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.start()

    ## Socket functions
    def start_server(self):
        BUFFER_SIZE = 1024
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(self.site_to_connection["cli"])
        sock.listen(4)
        incoming_list = []
        while len(incoming_list) != 1:
            conn, addr = sock.accept()
            conn.setblocking(0)
            incoming_list.append(conn)
        while True:
            for inc in incoming_list:
                try: #
                    data = inc.recv(BUFFER_SIZE)
                except:
                    continue
                if not data: break
                # if self.debug == True:
                #     print("Received: {0}".format(data)) # Alex Wu: Debugging purposes

                # Add data to the buffer.
                read_buffer = ""
                read_buffer += data
                # Split by the delimiter.
                parse = read_buffer.split("~")

                for data in parse:
                    if not data: break
                    # print("Received: {0}".format(data)) # Alex Wu: Debugging purposes
                    split = data.split()
                    if self.debug == True:
                        print "\nReceived:", data # Alex Wu: Debugging purposes
                    split = data.split()
                    if split[0] == "DECIDE":
                        self.q.put(1)
                inc.send(data)
            #conn.close()

    def message(self, message, component="prm"):
        BUFFER_SIZE = 1024
        RETRY_TIME = 2
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(self.site_to_connection[component])
                s.sendall(message)
                s.close()
                break
            except:
                print("Attempt to connect to " + str(self.site_to_connection[component]) +
                      " failed. Retrying in " + str(RETRY_TIME) + " seconds.")
                time.sleep(RETRY_TIME)

    def message_prm(self, message):
        self.socket_dict["prm"].sendall(message + "~")

    def connecting(self):
        RETRY_TIME = 2
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(self.site_to_connection["prm"])
                self.socket_dict["prm"] = sock
                break
            except:
                print("Attempt to connect to " + str(self.site_to_connection["prm"]) +
                      " failed. Retrying in " + str(RETRY_TIME) + " seconds.")
                time.sleep(RETRY_TIME)

    ## Data processing calls
    def do_map(self, line):
        input_list = line.split()
        if len(input_list) != 1:
            print("Usage: map filename")
            return
        filename = input_list[0]
        self.message("MAP {0} {1}".format(filename, 1), "map_a")
        self.message("MAP {0} {1}".format(filename, 2), "map_b")
        # time.sleep(5)
        # if self.q.empty():
        #     self.q.put(0)
        # if self.q.get() == 1:
        #     print("Map successful.")
        # else:
        #     print("Map failed.")
    def do_reduce(self, line):
        input_list = line.split()
        if len(input_list) < 1:
            print("Usage: reduce filename1 filename2 .....")
            return
        filenames = " ".join(input_list[0:])
        self.message("REDUCE {0}".format(filenames), "reduce")
        ## Wait for response or else timeout
        # time.sleep(5)
        # if self.q.empty():
        #     self.q.put(0)
        # if self.q.get() == 1:
        #     print("Reduce successful.")
        # else:
        #     print("Reduce failed.")

    def do_replicate(self, line):
        input_list = line.split()
        if len(input_list) != 1:
            print("Usage: replicate filename")
            return
        filename = input_list[0]
        if self.str_log:
            self.message_prm("REPLICATE {0}".format(filename))
            self.q.get()
        else:
            try:
                with open(filename, 'r') as f:
                    word_dict_str = f.readline()
                    word_dict_str = "".join(word_dict_str.split()) # To remove spaces
                    self.message_prm("REPLICATE {0} {1}".format(filename, word_dict_str))
                    time.sleep(5)
                    if self.q.empty():
                        self.q.put(0)
                    if self.q.get() == 1:
                        print("Replication successful.")
                    else:
                        print("Replication failed.")
            except:
                print("Couldn't read file")
    def do_stop(self, line):
        self.message_prm("STOP")
    def do_resume(self, line):
        self.message_prm("RESUME")

    ## Data query calls:
    ##     The CLI sends a query to the PRM, and the PRM prints the answers to these
    ##     queries in its stdout. There is no need to pass the results back to the CLI.
    def do_total(self, line):
        input_list = line.split()
        if len(input_list) < 1:
            print("Usage: total pos1 pos2 .....")
            return
        positions = input_list[0:]
        self.message_prm("TOTAL {0}".format(' '.join(positions)))
    def do_print(self, line):
        self.message_prm("PRINT")
    def do_merge(self, line):
        input_list = line.split()
        if len(input_list) != 2:
            print("Usage: merge pos1 pos2")
            return
        pos1 = input_list[0]
        pos2 = input_list[1]
        self.message_prm("MERGE {0} {1}".format(pos1, pos2))

    ## Help text
    def help_map(self):
        print '\n'.join(["map: map filename",
                         "    Splits the file based on its size into 2 equal parts. The split has to cut",
                         "    the file in a space character, not in the middle of a word. Then it maps",
                         "    each half to a mapper using message passing"])
    def help_reduce(self):
        print '\n'.join(["reduce: reduce filename1 filename2 .....",
                         "    Sends a message (using sockets) to the reducer with the provided filenames.",
                         "    The reducer has to reduce the intermediate files to a final reduced file"])
    def help_replicate(self):
        print '\n'.join(["replicate: replicate filename",
                         "    Sends a message to the PRM to replicate the file with other computing",
                         "    nodes. Notice that the PRM owns the log with all its log objects"])
    def help_stop(self):
        print '\n'.join(["stop: stop",
                         "    Moves the PRM to the stopped state. When the PRM in the stoppedstate, it",
                         "    does not handle any local replicate commands. In addition, it drops any log",
                         "    object replicating messages sent by other PRMs in other nodes. This is used",
                         "    to emulate failures and how Paxos can still achieve progress in the",
                         "    presence of (N/2 - 1) failures"])
    def help_resume(self):
        print '\n'.join(["resume: resume",
                         "    Resumes the PRM back to the active state. A PRM in the active state should",
                         "    actively handle local replicate commands as well as log object replicating",
                         "    messages received by other PRMs"])
    def help_total(self):
        print '\n'.join(["total: total pos1 pos2 .....",
                         "    Sums up the counts of all the word in all the log positions pos1 pos2, ..."])
    def help_print(self):
        print '\n'.join(["print: print",
                         "    Prints the filenames of all the log objects."])
    def help_merge(self):
        print "\n".join(["merge: merge pos1 pos2",
                         "    Apply the reduce function in log objects in positions pos1 pos2. In other",
                         "    words, it adds up the occurrence of words in log objects in positions pos1",
                         "    and pos2 and prints each word with its corresponding count."])

    ## Other comands
    def do_exit(self, line):
        """exit:\n    Exit the client API."""
        return True
    def default(self, line):
        print("invalid command: {0}".format(line))

    def do_EOF(self, line):
        print("exit")
        return True

    ## Ignore empty input (overrides the default -- repeating the last command entered)
    def emptyline(self): pass

    ## Say goodbye when the cli exits
    def postloop(self):
        print("Goodbye!")
        os._exit(0)


if __name__ == '__main__':
    main()

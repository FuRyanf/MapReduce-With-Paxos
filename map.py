#!/usr/bin/python
import argparse
import os
import socket
import threading

# To handle run two instances of map simultaneously, how to handle that? Like what would we pass in for the cli.
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("setup_file", type=str, help="configure with setup file")
    args = parser.parse_args()
    mapper = Map(args.setup_file)

class Map(object):
    def __init__(self, setup_file):
        self.site_to_connection = {}
        ## Parse with setupfile
        with open(setup_file, 'r') as f:
            try:
                ip_addr, port = f.readline().split()
                self.site_to_connection["cli"] = (ip_addr, int(port))
                ip_addr, port = f.readline().split()
                self.site_to_connection["map"] = ("0.0.0.0", int(port))
            except:
                print("ERROR: Can't read setup file")
        self.file_name = "file1"
        self.file_size = 0
        self.half = 0
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.start()

    def parse_file(self):
        self.file_size = self.get_size()
        if self.file_size == 0:
            self.write_file({}, self.half - 1)
            print("Warning: Empty file")
        elif self.half == 1:
            self.handle_first_half()
        elif self.half == 2:
            self.handle_second_half()
        else:
            print("ERROR: Invalid half")
        self.message_cli("DONE")
        # os._exit(1)

    def handle_first_half(self):
        with open(self.file_name) as f:
            try:
                data = ""
                char = ''
                for i in range(self.file_size/2):
                    char = f.read(1)
                    if not char:
                        break
                    data += char
                # Include the word we land on.
                while char.isspace() == False:
                    nextChar = f.read(1)
                    if not nextChar or nextChar.isspace():
                        break
                    data += nextChar
                # Generate lists by splitting the strings on spaces.
                data = data.split()
                # Convert to dict with counts.
                first_half = self.create_dict(data)
                # Write the file onto disk.
                self.write_file(first_half, 0)
            except:
                print("ERROR: Can't read file")

    def handle_second_half(self):
        with open(self.file_name) as f:
            try:
                self.file_size = self.get_size()
                if self.file_size == 1:
                    self.write_file({}, 1)
                    return
                # Read up to half the file.
                half = self.file_size/2
                start = half
                f.seek(half, 0)

                if f.read(1).isspace() == False:
                    # Check the previous character.
                    f.seek(half - 1, 0)
                    char = f.read(1)
                    while char.isspace() == False:
                        char = f.read(1)
                        if not char:
                            self.write_file({}, 1)
                            return
                        if char.isspace() == False:
                            start += 1
                # Seek to where we want to start reading.
                f.seek(start, 0)
                data = ""
                char = ''
                while True:
                    char = f.read(1)
                    if not char:
                        break
                    data += char

                # Generate lists by splitting the strings on spaces.
                data = data.split()
                # Convert to dict with counts.
                second_half = self.create_dict(data)
                # Write the file onto disk.
                self.write_file(second_half, 1)
            except:
                print("ERROR: Can't read file")


    def write_file(self, input_dict, file_id):
        written = str(input_dict)
        wr_file_name = '{0}_I_{1}'.format(self.file_name, file_id)
        f = open(wr_file_name, 'w')
        f.write(written)  # python will convert \n to os.linesep
        f.close()

    def get_size(self):
        st = os.stat(self.file_name)
        return st.st_size

    def create_dict(self, data):
        store = {}
        for word in data:
            if word not in store.keys():
                store[word] = 1
            else:
                store[word] += 1
        return store

    def start_server(self):
        BUFFER_SIZE = 1024
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("Binding to {0}".format(self.site_to_connection["map"]))
        sock.bind(self.site_to_connection["map"])
        sock.listen(1)
        while True:
            conn, addr = sock.accept()
            while True:
                try:
                    data = conn.recv(BUFFER_SIZE)
                except:
                    continue
                if not data: break

                # print("\nReceived: {0}".format(data))
                split = data.split()
                ## CLI Commands.
                if split[0] == "MAP":
                    self.file_name = split[1]
                    self.half = int(split[2])
                    self.parse_file()

                conn.send(data)
            conn.close()

    def message_cli(self, message):
        BUFFER_SIZE = 1024
        RETRY_TIME = 2
        if True:
            print("Sent: {0}".format(message)) # Alex Wu: Debugging purposes
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(self.site_to_connection["cli"])
                sock.sendall(message)
                sock.close()
                print(self.site_to_connection["cli"])
                # time.sleep(1)
                break
            except:
                print("Attempt to connect to " + str(self.site_to_connection["cli"]) +
                      " failed. Retrying in " + str(RETRY_TIME) + " seconds.")
                time.sleep(RETRY_TIME)


if __name__ == "__main__":
    main()

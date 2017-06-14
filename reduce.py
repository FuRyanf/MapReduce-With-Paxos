#!/usr/bin/python
import argparse
import ast
import os
import socket
import threading
from collections import defaultdict

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("setup_file", type=str, help="configure with setup file")
    args = parser.parse_args()
    reducer = Reduce(args.setup_file)

class Reduce(object):
    def __init__(self, setup_file):
        self.site_to_connection = {}
        ## Parse with setupfile
        with open(setup_file) as f:
            try:
                ip_addr, port = f.readline().split()
                self.site_to_connection["cli"] = (ip_addr, int(port))
                ip_addr, port = f.readline().split()
                self.site_to_connection["reduce"] = (ip_addr, int(port))
            except:
                print("ERROR: Can't read setup file")
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.start()

    def parse_files(self, file_names):
        dict_data_list = []

        for file_name in file_names:
            try:
                with open(file_name, 'r') as my_file:
                    string = my_file.read()
                    file_dict = self.string_to_dict(string)
                    dict_data_list.append(file_dict)
            except:
                print('ERROR: file_name {0} not found'.format(file_name))

        combined_dict = self.dict_sum(dict_data_list)

        ## File_name would be equal to the first file_ strip everything before the _I_0
        # and then append it _reduced
        file_name_reduced = file_names[0].split("_I_")[0] + "_reduced"

        self.write_file(combined_dict, file_name_reduced)

        self.message_cli("DONE")
        # os._exit(1)

    def string_to_dict(self, str):
        return ast.literal_eval(str)

    def write_file(self, input_dict, file_name):
        written = str(input_dict)
        f = open(file_name, 'w')
        f.write(written)
        f.close()

    def dict_sum(self, dicts):
        ret = defaultdict(int)
        for d in dicts:
            for k, v in d.items():
                ret[k] += v
        return dict(ret)

    # Only the local CLI should connect map's server.
    def start_server(self):
        BUFFER_SIZE = 1024
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("Binding to {0}".format(self.site_to_connection["reduce"]))
        sock.bind(self.site_to_connection["reduce"])
        sock.listen(1)
        while True:
            conn, addr = sock.accept()
            while True:
                try:
                    data = conn.recv(BUFFER_SIZE)
                except:
                    continue
                if not data: break

                split = data.split()
                ## CLI Commands.
                if split[0] == "REDUCE":
                    self.parse_files(split[1:]) # pass the rest of the files in as a list

                conn.send(data)
            conn.close()

    def message_cli(self, message):
        BUFFER_SIZE = 1024
        RETRY_TIME = 2
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(self.site_to_connection["cli"])
                sock.sendall(message)
                sock.close()
                break
            except:
                print("Attempt to connect to " + str(self.site_to_connection["cli"]) +
                      " failed. Retrying in " + str(RETRY_TIME) + " seconds.")
                time.sleep(RETRY_TIME)


if __name__ == "__main__":
    main()

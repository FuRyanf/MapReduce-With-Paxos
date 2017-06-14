# cs171 asg3 (Final Project)
Map Reduce Replicate
by Alexander Wu and Ryan Fu
---------------------------

To run our program, open up 3 terminals. In each of these terminals, make sure you are in the project folder with . In terminal 1:

    ./bin/pub1
    ./bin/cp_priv
    ./bin/paxos1

For terminal 2:

    ./bin/priv2
    ./bin/paxos2

For terminal 3:

    ./bin/priv3
    ./bin/paxos3

If everything ran smoothly, we should be in the CLI (Command Line Interface) in all three terminals. pub1 is a script file that copies relevent files to our public Eucalytus computer and does an ssh to it. If you run into a problem, try `chmod 400 cs171Key.pem`. cp_priv copies the files from our public Eucalytus comptuter to our two private Eucalyptus computers (we can't directly scp files from our local machine since the private Eucalytus computers have private IP addresses). priv1 does an ssh to the public server then does an ssh from there to the private server. paxos1 is a script file that runs all the 2 mappers, 1 reducer, and the PRM (Paxos Replication Module) in the background and runs the CLI in the foreground. Alternatively, one could type into terminal:

    ./map.py setup/map_a_setup.txt &
    ./map.py setup/map_b_setup.txt &
    ./reduce.py setup/reduce_setup.txt &
    ./prm.py setup/prm_1_3_setup.txt &
    ./cli.py setup/cli_1_setup.txt

paxos2 does the same thing for the 2nd node. It is identical to typing in terminal:

    ./map.py setup/map_a_setup.txt &
    ./map.py setup/map_b_setup.txt &
    ./reduce.py setup/reduce_setup.txt &
    ./prm.py setup/prm_2_3_setup.txt &
    ./cli.py setup/cli_2_setup.txt

paxos 3 does the same thing for the 3rd node. It is identical to typing in terminal:

    ./map.py setup/map_a_setup.txt &
    ./map.py setup/map_b_setup.txt &
    ./reduce.py setup/reduce_setup.txt &
    ./prm.py setup/prm_3_3_setup.txt &
    ./cli.py setup/cli_3_setup.txt


Now go back to the CLI and type in whatever commands you would like. Type 'help' for a list of commands and 'help _command_' for more detail on a particular comand. Our CLI offers tab completion, so you can type in 'rep' and then pressing tab will autocomplete this to 'replicate'.

# Status of project

We have finished the project.

If we had more time, we would have implemented the phase 1/2 optimization where a leader is chosen after a successful round of paxos.

## Eucalyptus setup
  | Node        | IP Address     |
  |-------------|----------------|
  | 1 (Public)  | 128.111.84.196 |
  | 2 (Private) | 10.2.96.169    |
  | 3 (Private) | 10.2.96.186    |

  | Component | Port | Setup command                      |
  |-----------|------|------------------------------------|
  | CLI_1     | 5000 | ./cli.py setup/cli_1_setup.txt     |
  | CLI_2     | 5000 | ./cli.py setup/cli_2_setup.txt     |
  | CLI_3     | 5000 | ./cli.py setup/cli_3_setup.txt     |
  | Mapper_1a | 5001 | ./map.py setup/map_a_setup.txt     |
  | Mapper_2a | 5001 | ./map.py setup/map_a_setup.txt     |
  | Mapper_3a | 5001 | ./map.py setup/map_a_setup.txt     |
  | Mapper_1b | 5002 | ./map.py setup/map_b_setup.txt     |
  | Mapper_2b | 5002 | ./map.py setup/map_b_setup.txt     |
  | Mapper_3b | 5002 | ./map.py setup/map_b_setup.txt     |
  | Reducer_1 | 5003 | ./reduce.py setup/reduce_setup.txt |
  | Reducer_2 | 5003 | ./reduce.py setup/reduce_setup.txt |
  | Reducer_3 | 5003 | ./reduce.py setup/reduce_setup.txt |
  | PRM_1     | 5004 | ./prm.py setup/prm_1_3_setup.txt   |
  | PRM_2     | 5004 | ./prm.py setup/prm_2_3_setup.txt   |
  | PRM_3     | 5004 | ./prm.py setup/prm_3_3_setup.txt   |

## Local setup
  Alternatively, you can setup our project locally. Nodes are configured with IP Address 127.0.0.1 (localhost).

  | Component | Port | Setup command                              |
  |-----------|------|--------------------------------------------|
  | CLI_1     | 5010 | ./cli.py local_setup/cli_1_setup.txt       |
  | CLI_2     | 5020 | ./cli.py local_setup/cli_2_setup.txt       |
  | CLI_3     | 5030 | ./cli.py local_setup/cli_3_setup.txt       |
  | Mapper_1a | 5011 | ./map.py local_setup/map_1a_setup.txt      |
  | Mapper_2a | 5021 | ./map.py local_setup/map_1b_setup.txt      |
  | Mapper_3a | 5031 | ./map.py local_setup/map_1c_setup.txt      |
  | Mapper_1b | 5012 | ./map.py local_setup/map_2a_setup.txt      |
  | Mapper_2b | 5022 | ./map.py local_setup/map_2b_setup.txt      |
  | Mapper_3b | 5032 | ./map.py local_setup/map_2c_setup.txt      |
  | Reducer_1 | 5013 | ./reduce.py local_setup/reduce_1_setup.txt |
  | Reducer_2 | 5023 | ./reduce.py local_setup/reduce_2_setup.txt |
  | Reducer_3 | 5033 | ./reduce.py local_setup/reduce_3_setup.txt |
  | PRM_1     | 5014 | ./prm.py local_setup/prm_1_3_setup.txt     |
  | PRM_2     | 5024 | ./prm.py local_setup/prm_2_3_setup.txt     |
  | PRM_3     | 5034 | ./prm.py local_setup/prm_3_3_setup.txt     |

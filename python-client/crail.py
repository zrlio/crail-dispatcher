#######################################################
##  Python API to communicate with Crail dispatcher  ##
#######################################################

import time
import sys
import os
import socket
import struct
import errno
from subprocess import call, Popen

PORT = 2345
HOSTNAME = "localhost"
CMD_PUT = 0
CMD_GET = 1
CMD_DEL = 2
CMD_CREATE_DIR = 3

def setup_dirs():
  call(["mkdir", "/tmp/hugepages"]) 
  call(["mkdir", "/tmp/hugepages/cache"]) 
  call(["mkdir", "/tmp/hugepages/data"]) 



def setup_env(crail_home_path):
  os.environ["JAVA_HOME"] = "/usr/lib/jvm/jre-1.8.0-openjdk.x86_64"
  os.environ["CRAIL_HOME"] = crail_home_path
  os.environ["CLASSPATH"] = crail_home_path + "jars/crail-client-1.0.jar:" + crail_home_path + \
                             "jars/reflex-client-1.0-jar-with-dependencies.jar:" + crail_home_path + \
                             "jars/log4j.properties:" + crail_home_path + "jars/crail-dispatcher-1.0.jar"
  print "CLASSPATH = " , os.environ["CLASSPATH"]
  return

# call this from lambda environment (working directory is /var/task)
def launch_dispatcher_from_lambda():
  setup_env("/var/task/")
  setup_dirs()
  Popen(["java", "-cp", "/var/task/jars/*", 
	         "-Dlog4j.configuration=file:///var/task/conf/log4j.properties", 
		 "com.ibm.crail.dispatcher.CrailDispatcher"])
  return 

# use this for customized CRAIL_HOME directory
def launch_dispatcher(crail_home_path):
  setup_env(crail_home_path)
  Popen(["java", "-cp", crail_home_path + "/jars/*", 
	         "-Dlog4j.configuration=file://" + crail_home_path + "/conf/log4j.properties", 
		 "com.ibm.crail.dispatcher.CrailDispatcher"])
  return 

def connect():
  try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOSTNAME, PORT))
  except socket.error as e:
    if e.errno == errno.ECONNREFUSED:
      print "Connection refused -- did you launch_dispatcher?"
      return None
    else:
      raise
      return None
  print "Connected to crail dispatcher." 

  return s
 
 
def pack_msg(src_filename, dst_filename, ticket, cmd):
  src_filename_len = len(src_filename) 
  dst_filename_len = len(dst_filename)
  msg_packer = struct.Struct("!iqhi" + str(src_filename_len) + "si" + str(dst_filename_len) + "s")
  msg_len = 2 + 4 + src_filename_len + 4 + dst_filename_len

  msg = (msg_len, ticket, cmd, src_filename_len, src_filename, dst_filename_len, dst_filename)
  pkt = msg_packer.pack(*msg)

  return pkt

def put(socket, src_filename, dst_filename, ticket):  
  '''
  Send a PUT request to Crail to write key

  :param socket:           socket with established connection to crail dispatcher
  :param str src_filename: name of local file containing data to PUT
  :param str dst_filename: name of file/key in Crail which writing to
  :param int ticket:       value greater than 0, unique to each connection
  :return: the Crail dispatcher response 
  '''
  pkt = pack_msg(src_filename, dst_filename, ticket, CMD_PUT) 

  socket.sendall(pkt) 
  data = socket.recv(4)

  return data

 
def get(socket, src_filename, dst_filename, ticket):  
  '''
  Send a GET request to Crail to read key

  :param socket:           socket with established connection to crail dispatcher
  :param str src_filename: name of file/key in Crail from which reading
  :param str dst_filename: name of local file where want to store data from GET
  :param int ticket:       value greater than 0, unique to each connection
  :return: the Crail dispatcher response 
  '''
  pkt = pack_msg(src_filename, dst_filename, ticket, CMD_GET) 

  socket.sendall(pkt) 
  data = socket.recv(4)

  return data

def delete(socket, src_filename, ticket):  
  '''
  Send a DEL request to Crail to delete key

  :param socket:           socket with established connection to crail dispatcher
  :param str src_filename: name of file/key in Crail which deleting
  :param int ticket:       value greater than 0, unique to each connection
  :return: the Crail dispatcher response 
  '''
  src_filename_len = len(src_filename)
  msg_packer = struct.Struct("!iqhi" + str(src_filename_len) + "si")
  msg_len = 2 + 4 + src_filename_len + 4 

  msg = (msg_len, ticket, CMD_DEL, src_filename_len, src_filename, 0)
  pkt = msg_packer.pack(*msg)

  socket.sendall(pkt) 
  data = socket.recv(4)

  return data


def create_dir(socket, src_filename, ticket):  
  '''
  Send a CREATE DIRECTORY request to Crail

  :param socket:           socket with established connection to crail dispatcher
  :param str src_filename: name of directory to create in Crail 
  :param int ticket:       value greater than 0, unique to each connection
  :return: the Crail dispatcher response 
  '''
  src_filename_len = len(src_filename)
  msg_packer = struct.Struct("!iqhi" + str(src_filename_len) + "si")
  msg_len = 2 + 4 + src_filename_len + 4 

  print "crail.py: create dir...", src_filename

  msg = (msg_len, ticket, CMD_CREATE_DIR, src_filename_len, src_filename, 0)
  pkt = msg_packer.pack(*msg)

  socket.sendall(pkt) 
  data = socket.recv(4)

  return data

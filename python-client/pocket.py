#######################################################
##  Python library/API to communicate with Pocket    ##
#######################################################

import time
import sys
import os
import socket
import struct
import errno
import libpocket
from subprocess import call, Popen

PORT = 2345
HOSTNAME = "localhost"

def launch_dispatcher_from_lambda():
  return 

def launch_dispatcher(crail_home_path):
  return 

# TODO: add heuristics
def register_job(pocket, jobid):
  res = pocket.MakeDir(jobid)
  if res != 0:
    print("Error registering job!")
  return res

def connect(hostname, port):
  pocketHandle = libpocket.PocketDispatcher()
  res = pocketHandle.Initialize(hostname, port)
  if res != 0:
    print("Connecting to metadata server failed!")

  return pocketHandle

def put(pocket, src_filename, dst_filename, jobid, PERSIST_AFTER_JOB=False):  
  '''
  Send a PUT request to Pocket to write key

  :param pocket:           pocketHandle returned from connect()
  :param str src_filename: name of local file containing data to PUT
  :param str dst_filename: name of file/key in Pocket which writing to
  :param str jobid:        id unique to this job, used to separate keyspace for job
  :param PERSIST_AFTER_JOB:optional hint, if True, data written to table persisted after job done
  :return: the Pocket dispatcher response 
  '''

  if PERSIST_AFTER_JOB:
    set_filename = jobid + "-persist/" + dst_filename
  else:
    set_filename = jobid + "/" + dst_filename

  res = pocket.PutFile(src_filename, set_filename)

  return res

 
def get(pocket, src_filename, dst_filename, jobid, DELETE_AFTER_READ=False):  
  '''
  Send a GET request to Pocket to read key

  :param pocket:           pocketHandle returned from connect()
  :param str src_filename: name of file/key in Pocket from which reading
  :param str dst_filename: name of local file where want to store data from GET
  :param str jobid:        id unique to this job, used to separate keyspace for job
  :param DELETE_AFTER_READ:optional hint, if True, data deleted after job done
  :return: the Pocket dispatcher response 
  '''

  get_filename = jobid + "/" + src_filename

  res = pocket.GetFile(get_filename, dst_filename)
  if res != 0:
    print("GET failed!")
    return res

  if DELETE_AFTER_READ:
    res = delete(pocket, src_filename, jobid);

  return res


def delete(pocket, src_filename, jobid):  
  '''
  Send a DEL request to Pocket to delete key

  :param pocket:           pocketHandle returned from connect()
  :param str src_filename: name of file/key in Pocket which deleting
  :param str jobid:        id unique to this job, used to separate keyspace for job
  :return: the Pocket dispatcher response 
  '''
  
  if src_filename:
    src_filename = jobid + "/" + src_filename
  else:
    src_filename = jobid

  res = pocket.DeleteFile(src_filename) #FIXME: or DeleteDir if want recursive delete always?? 
  
  return res


def create_dir(pocket, src_filename, jobid):  
  '''
  Send a CREATE DIRECTORY request to Pocket

  :param pocket:           pocketHandle returned from connect()
  :param str src_filename: name of directory to create in Pocket 
  :param str jobid:        id unique to this job, used to separate keyspace for job
  :return: the Pocket dispatcher response 
  '''
  
  if src_filename:
    src_filename = jobid + "/" + src_filename
  else:
    src_filename = jobid

  res = pocket.MakeDir(src_filename)

  return res


def close(pocket):  
  '''
  Send a CLOSE request to PocketFS

  :param pocket:           pocketHandle returned from connect()
  :return: the Pocket dispatcher response 
  '''
  return pocket.Close() #TODO

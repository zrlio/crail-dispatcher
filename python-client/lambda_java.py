## Simple lambda microbenchmark for unloaded latency tests

import time
import sys
import os
import crail
from subprocess import call, Popen

def handler(event, context):
  crail.launch_dispatcher_from_lambda()
  
  call(["cp", "/var/task/lambda_java.py", "/tmp/lambda"]) 
  call(["cp", "/var/task/jars/crail-reflex-1.0.jar", "/tmp/crail-reflex"]) 

  time.sleep(20)
  
  socket = crail.connect()
  
  print "Talk to dispatcher..."
  src_filename = "/tmp/crail-reflex"
  dst_filename = "/dsttest-test-reflex2.data"
  ticket = 1001
  print "Try PUT..."
  start = time.time()
  crail.put(socket, src_filename, dst_filename, ticket)
  end = time.time()
  print "Execution time for single PUT: ", (end-start) * 1000000, " us\n"
  
  time.sleep(1)
  src_filename = "/dsttest-test-reflex2.data"
  dst_filename = "/tmp/crail-reflex-2"
  print "Now GET..."
  start = time.time()
  crail.get(socket, src_filename, dst_filename, ticket)
  end = time.time()
  print "Execution time for single GET: ", (end-start) * 1000000, " us\n"

  
  time.sleep(1)
  call(["ls", "-al", "/tmp/"])
  
  src_filename = "/dsttest-test-reflex2.data"
  print "Now DEL..."
  start = time.time()
  crail.delete(socket, src_filename, ticket)
  end = time.time()
  print "Execution time for single GET: ", (end-start) * 1000000, " us\n"

  return

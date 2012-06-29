#!/usr/bin/python

'''Reads HA logs and prints an easy-to-read representation
of how master elections have occurred.
'''

import re
from datetime import datetime
from time import mktime

#2012-05-30 09:28:56.233-0700: Starting[103] as slave
ELECTION_STARTED_REGEX = re.compile('.*master-notify set to (\d+).*')
MASTER_REBOUND_REGEX = re.compile('.*master-rebound set to (\d+).*')
STARTING_AS_REGEX = re.compile('.*Starting\[(\d+)\] as.*')

OPENED_LOG_REGEXES = (
  re.compile('.*Opened .+ clean empty log, version=.+, lastTxId=(\d+).*'),
  re.compile('.*Opened .+ clean empty log, version=.+, lastTxId=(\d+).*'),
  re.compile('.*Internal recovery completed, scanned .+ log entries. Recovered .+ transactions. Last tx recovered: (\d+).*'),
  re.compile('.*Opened logical log .+ version=.+, lastTx=(\d+).*'),
)

STARTUP_FIRST_TIME_REGEX = re.compile('.*newMaster called Starting up for the first time.*')
SHUTDOWN_REGEX = re.compile('.*Shutdown\[(\d+)\],')

BRANCHED_DATA_REGEX = re.compile( ".*Branched data occurred.*" )

ZOOCLIENT_INFO_REGEX = re.compile( '.*ZooClient\[serverId:(\d+).*' )

MASTER = "master"
SLAVE = "slave"

class ElectionCycle(object):
  ''' Represents one election cycle.
  '''

  def __init__(self, timestamp, started_by):
    self.timestamp = timestamp
    self.type_switches = []
    self.started_by = started_by

  def __str__(self):
    return "\n".join(self.get_log_messages())

  def get_log_messages(self):
    out = ["%s Election started by %s" % (self.timestamp, self.started_by)]
    # Naive way to check if a server has become master when another server
    # has a higher tx id. TODO: Refactor this to handle cases where a 
    # master with a high id goes down, or when the whole cluster has been reset.
    max_tx_switch = ServerTypeSwitch(0, -1, SLAVE, -1)
    prev_switch_tx_id = 0
    for switch in sorted(self.type_switches, key=lambda switch:switch.timestamp):
      # Figure out tx id change from previous server
      tx_id_delta = switch.tx_id - prev_switch_tx_id
      tx_id_delta = "+%d" % tx_id_delta if tx_id_delta >= 0 else tx_id_delta
      prev_switch_tx_id = switch.tx_id

      out.append( "%s (%s)" % (switch, tx_id_delta) )

      # Keep track of which server has the highest TX id
      if max_tx_switch.tx_id < switch.tx_id:
        max_tx_switch = switch
      
      # If this server is becoming master, make
      # sure no other server has a higher tx id.
      if switch.mode == MASTER and max_tx_switch.tx_id > switch.tx_id:
        delta = max_tx_switch.tx_id - switch.tx_id
        out[len(out)-1] += "   WARN: Master is %s transactions behind server %s" % (delta, max_tx_switch.server_id)
    return out

  def add_type_switch(self, type_switch):
    self.type_switches.append(type_switch)

class ServerTypeSwitch(object):
  
  def __init__(self, timestamp, server_id, mode, tx_id):
    self.timestamp = timestamp
    self.server_id = server_id
    self.mode = mode
    self.tx_id = tx_id

  def __str__(self):
    # Pad "slave" to align with "master"
    mode = MASTER if self.mode == MASTER else SLAVE + " "
    return "%s   %s became %s Last TX: %s" % (self.timestamp, self.server_id, mode, self.tx_id)


def main(args):
  if len(args) <= 1:
    print """Usage: electionhistory MESSAGES_LOG_LOCATION [MESSAGES_LOG_LOCATION ..]

Bash lets you use patterns to match multiple logs, for instance:
electionhistory mylogs*/messages.log"""

  log_paths = args[1:]

  # First, find election cycles and general events
  election_cycles = []
  output_messages = []
  for log_path in log_paths:
    with open(log_path) as logfile:
      # Election cycles
      for election_cycle in find_election_cycles(logfile):
        election_cycles.append(election_cycle)

      # Rewind
      logfile.seek(0)

      # General events
      current_server_id = "Unknown server"
      for line in logfile:
        
        match = ZOOCLIENT_INFO_REGEX.match(line)
        if match:
          current_server_id = match.groups()[0]
          continue

        match = MASTER_REBOUND_REGEX.match(line)
        if match:
          output_messages.append("%s     [Master rebound: %s]" % (parse_timestamp(line), match.groups()[0]))
          continue
        
        match = STARTUP_FIRST_TIME_REGEX.match(line)
        if match:
          output_messages.append("%s     [Startup: %s]" % (parse_timestamp(line), current_server_id))
          continue
        
        match = SHUTDOWN_REGEX.match(line)
        if match:
          output_messages.append("%s     [Shutdown: %s]" % (parse_timestamp(line), match.groups()[0]))
          continue
        
        match = BRANCHED_DATA_REGEX.match(line)
        if match:
          output_messages.append("%s     [%s]" % (parse_timestamp(line), line[30:-1]))
          continue
        

  election_cycles.sort(key=lambda election:election.timestamp)
  election_cycles.reverse()

  if len(election_cycles) is 0:
    print "No election cycles found."
    return

  # Second, find all server type switches (MASTER/SLAVE)
  # This is done separately, because we want to add these
  # events to the appropriate election cycle in order to
  # do correctness checking and so on within that cycle.
  for log_path in log_paths:
    with open(log_path) as logfile:

      loglines = iter(logfile)

      for line in loglines:

        tx_id = find_last_committed_tx(loglines)

        type_switch = find_next_type_switch(loglines, tx_id)

        if type_switch is not None:
          # Add to appropriate election cycle 
          for election in election_cycles:
            if election.timestamp < type_switch.timestamp:
              election.add_type_switch(type_switch)
              break 

  # Sort by timestamp
  #type_switches.sort(key=lambda switch: switch.timestamp)

  # Third, add all the entries we have together,
  # and sort them by timestamp
  for election in election_cycles:
    output_messages += election.get_log_messages()

  output_messages.sort()
  print "\n".join(output_messages)

def find_election_cycles(loglines):
  for line in loglines:
    match = ELECTION_STARTED_REGEX.match(line)
    if match:
      server_id = match.groups()[0]
      yield ElectionCycle(parse_timestamp(line), server_id)

def find_last_committed_tx(loglines):
  for line in loglines:
    for regex in OPENED_LOG_REGEXES:
      match = regex.match(line)
      if match:
        return int(match.groups()[0])
  return -1
  

def find_next_type_switch(loglines, tx_id):
  for line in loglines:
    match = STARTING_AS_REGEX.match(line)
    if match:
      if line.endswith("as master\n"):
        mode = MASTER
      if line.endswith("as slave\n"):
        mode = SLAVE

      server_id = match.groups()[0]
      timestamp = parse_timestamp(line)

      return ServerTypeSwitch(timestamp, server_id, mode, tx_id)
  return None


def parse_timestamp(logline):
  # 2012-05-30 09:28:56.233-0700
  raw = logline[:len("2012-05-30 09:28:56.233-0700")]
  # TZ parsing is not supported on all python platforms, do it manually
  tz = int(raw[-5:-2]) # -0700, parse -07 as TZ hour offset
  raw = raw[:-5]

  # Ignore TZ for now. All that work for no reason.

  # Parse the TZ-free date
  return datetime.strptime(raw,"%Y-%m-%d %H:%M:%S.%f")

if __name__ == "__main__":
  import sys
  main(sys.argv)

#!/usr/bin/python

'''Reads HA logs and prints an easy-to-read representation
of how master elections have occurred.
'''

import re
from datetime import datetime
from time import mktime

#2012-05-30 09:28:56.233-0700: Starting[103] as slave

STARTING_AS_REGEX = re.compile('.*Starting\[(\d+)\] as.*')
LAST_TX_ID_REGEXES = (
  re.compile('.*Opened .+ clean empty log, version=.+, lastTxId=(\d+).*'),
  re.compile('.*Opened .+ clean empty log, version=.+, lastTxId=(\d+).*'),
  re.compile('.*Internal recovery completed, scanned .+ log entries. Recovered .+ transactions. Last tx recovered: (\d+).*'),
  re.compile('.*Opened logical log .+ version=.+, lastTx=(\d+).*'),
)

MASTER = "master"
SLAVE = "slave"

class ServerTypeSwitch(object):
  
  def __init__(self, timestamp, server_id, mode, last_tx_id):
    self.timestamp = timestamp
    self.server_id = server_id
    self.mode = mode
    self.last_tx_id = last_tx_id

  def __str__(self):
    # Pad "slave" to align with "master"
    mode = MASTER if self.mode == MASTER else SLAVE + " "
    return "%s %s started as %s Last TX: %s" % (self.timestamp, self.server_id, mode, self.last_tx_id)


def main(args):
  if len(args) <= 1:
    print """Usage: electionhistory MESSAGES_LOG_LOCATION [MESSAGES_LOG_LOCATION ..]

Bash lets you use patterns to match multiple logs, for instance:
electionhistory mylogs*/messages.log"""

  log_paths = args[1:]

  type_switches = []

  for log_path in log_paths:
    with open(log_path) as logfile:
      loglines = iter(logfile)

      for line in loglines:
        match = STARTING_AS_REGEX.match(line)
        if match:
          if line.endswith("as master\n"):
            mode = SLAVE
          if line.endswith("as slave\n"):
            mode = MASTER

          server_id = match.groups()[0]
          timestamp = parse_timestamp(line)

          # Find last committed TX ID
          for line in loglines:
            for regex in LAST_TX_ID_REGEXES:
              match = regex.match(line)
              if match:
                break
            if match:              
              last_tx_id = match.groups()[0]
              break
          else:
            last_tx_id="Reached end of log without finding last_tx_id entry"
          type_switches.append(ServerTypeSwitch(timestamp, server_id, mode, last_tx_id))

  # Sort by timestamp
  type_switches.sort(key=lambda switch: switch.timestamp)

  # Naive way to check if a server has become master when another server
  # has a higher tx id. TODO: Refactor this to handle cases where a 
  # master with a high id goes down, or when the whole cluster has been reset.
  max_tx_switch = ServerTypeSwitch(0, -1, SLAVE, -1)
  prev_max_tx_id = 0
  for switch in type_switches:
    tx_id_delta = (int(switch.last_tx_id) - prev_max_tx_id)
    tx_id_delta = ("+%d" % tx_id_delta if tx_id_delta >= 0 else tx_id_delta)
    prev_max_tx_id = int(switch.last_tx_id)
    print "%s (%s)" % (switch, tx_id_delta)
    # Keep track of which server has the highest TX id
    if max_tx_switch.last_tx_id < switch.last_tx_id:
      max_tx_switch = switch
    
    # If this server is becoming master, make
    # sure no other server has a higher tx id.
    if switch.mode == MASTER and max_tx_switch.last_tx_id > switch.last_tx_id:
      print "  WARN: This master has a lower TX id than server %s (%s < %s)" % (max_tx_switch.server_id, switch.last_tx_id, max_tx_switch.last_tx_id)
          
def parse_timestamp(logline):
  # 2012-05-30 09:28:56.233-0700
  raw = logline[:len("2012-05-30 09:28:56.233-0700")]
  # TZ parsing is not supported on all python platforms, do it manually
  tz = int(raw[-5:-2]) # -0700, parse -07 as TZ hour offset
  raw = raw[:-5]

  # Ignore TZ for now. All that work for no reason.

  # Parse the TZ-free date
  parsed = datetime.strptime(raw,"%Y-%m-%d %H:%M:%S.%f")

  return mktime(parsed.timetuple())

if __name__ == "__main__":
  import sys
  main(sys.argv)

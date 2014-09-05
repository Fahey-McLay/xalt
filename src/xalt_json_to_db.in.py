#!/usr/bin/env python
# -*- python -*-
#
#  xalt_json_to_db takes the output found in the ~/.xalt.d/[link,run]*
#  output files and puts it into the database
#
#  optional input:
#    XALT_USERS:  colon separated list of users; only these users are 
#       considered instead of all
#
#
# Git Version: @git@

from __future__  import print_function
import os, sys, re, MySQLdb, json, time, argparse
dirNm, execName = os.path.split(os.path.realpath(sys.argv[0]))
sys.path.insert(1,os.path.realpath(os.path.join(dirNm, "../libexec")))
sys.path.insert(1,os.path.realpath(os.path.join(dirNm, "../site")))

from XALTdb        import XALTdb
from XALTdb2       import *
from xalt_rmap_util import *
from xalt_site_pkg import translate
from xalt_util     import files_in_tree, capture, config_logger, passwd_generator, remove_files
from progressBar   import ProgressBar
import warnings, getent
warnings.filterwarnings("ignore", "Unknown table.*")

ConfigBaseNm = "xalt_db"
ConfigFn     = ConfigBaseNm + ".conf"
logger       = config_logger()

class CmdLineOptions(object):
  def __init__(self):
    pass
  
  def execute(self):
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete",      dest='delete', action="store_true", help="delete files after reading")
    parser.add_argument("--timer",       dest='timer',  action="store_true", help="Time runtime")
    parser.add_argument("--reverseMapD", dest='rmapD',  action="store",      help="Path to the directory containing the json reverseMap")
    args = parser.parse_args()
    return args


def link_json_to_db(xalt, user, reverseMapT, linkFnA):

  num = 0

  try:
    for fn in linkFnA:
      num  += 1
      f     = open(fn,"r")
      try:
        linkT = json.loads(f.read())
      except:  
        f.close()
        continue
      f.close()

      link_to_db(xalt, reverseMapT, linkT)

  except MySQLdb.Error, e:
    print ("Error %d: %s" % (e.args[0], e.args[1]))
    sys.exit (1)
  return num


def run_json_to_db(xalt, user, reverseMapT, runFnA):
  nameA = [ 'num_cores', 'num_nodes', 'account', 'job_id', 'queue' ]
  num   = 0
  try:
    for fn in runFnA:
      num   += 1
      f      = open(fn,"r")
      
      try:
        runT   = json.loads(f.read())
      except:
        f.close()
        continue
      f.close()

      run_to_db(xalt, reverseMapT, runT)

  except MySQLdb.Error, e:
    print ("Error %d: %s" % (e.args[0], e.args[1]))
    sys.exit (1)
  return num



def main():

  args   = CmdLineOptions().execute()
  xalt   = XALTdb(ConfigFn)

  num    = int(capture("getent passwd | wc -l"))
  pbar   = ProgressBar(maxVal=num)
  icnt   = 0

  t1     = time.time()

  rmapT  = Rmap(args.rmapD).reverseMapT()

  iuser  = 0
  lnkCnt = 0
  runCnt = 0

  for user, hdir in passwd_generator():
    xaltDir = os.path.join(hdir,".xalt.d")
    if (os.path.isdir(xaltDir)):
      iuser   += 1
      linkFnA  = files_in_tree(xaltDir, "*/link.*.json")
      lnkCnt  += link_json_to_db(xalt, user, rmapT, linkFnA)
      if (args.delete):
        remove_files(linkFnA)
        remove_files(files_in_tree(xaltDir, "*/.link.*.json"))

      runFnA   = files_in_tree(xaltDir, "*/run.*.json")
      runCnt  += run_json_to_db(xalt, user, rmapT, runFnA)
      if (args.delete):
        remove_files(runFnA)
        remove_files(files_in_tree(xaltDir, "*/.run.*.json"))
    icnt += 1
    pbar.update(icnt)

  xalt.connect().close()
  pbar.fini()
  t2 = time.time()
  rt = t2 - t1
  if (args.timer):
    print("Time: ", time.strftime("%T", time.gmtime(rt)))

  print("num users: ", iuser, ", num links: ", lnkCnt, ", num runs: ", runCnt)

if ( __name__ == '__main__'): main()

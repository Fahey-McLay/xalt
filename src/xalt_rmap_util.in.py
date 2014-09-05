#
#  utilities for obtaining modulename by using a file with a reverseMap
#
# Git Version: @git@

from __future__  import print_function
import os, sys, re, json, time, argparse
dirNm, execName = os.path.split(os.path.realpath(sys.argv[0]))
sys.path.append(os.path.realpath(os.path.join(dirNm, "../libexec")))
sys.path.append(os.path.realpath(os.path.join(dirNm, "../site")))


numberPat = re.compile(r'[0-9][0-9]*')
def obj_type(object_path):
  result = None
  a      = object_path.split('.')
  for entry in reversed(a):
    m = numberPat.search(entry)
    if (m):
      continue
    else:
      result = entry
      break
  return result


defaultPat = re.compile(r'default:?')
def obj2module(object_path, reverseMapT):
  dirNm, fn  = os.path.split(object_path)
  moduleName = 'NULL'
  pkg         = reverseMapT.get(dirNm)
  if (pkg):
    flavor    = pkg['flavor'][0]
    flavor    = defaultPat.sub('',flavor)
    if (flavor):
      moduleName = "'" + pkg['pkg'] + '(' + flavor + ")'"
    else:
      moduleName = "'" + pkg['pkg'] + "'"
  return moduleName


class Rmap(object):
  def __init__(self, rmapD):
    self.__rmapT = {}
    if (not rmapD):
      return
    rmapA   = rmapD.split(':')
    searchA = [ ".json", ".old.json"]
    rmapFn  = None
    for dir in rmapA:
      for ext in searchA:
        rmapFn = os.path.join(dir, "jsonReverseMapT" + ext)
        if (os.access(rmapFn, os.R_OK)):
          break
    if (rmapFn):
      rmpMtime = os.stat(rmapFn).st_mtime
      f        = open(rmapFn,"r")
      t        = json.loads(f.read())
      f.close()
      tsFn     = t.get('timestampFn')
      if (tsFn):
        tsMtime = os.stat(tsFn).st_mtime
        if (rmpMtime >= tsMtime):
          self.__rmapT = t['reverseMapT']
      else:
        self.__rmapT = t['reverseMapT']
        

  def reverseMapT(self):
    return self.__rmapT 



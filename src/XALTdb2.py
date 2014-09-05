from __future__  import print_function
import os, sys, re, MySQLdb, json, time, argparse
dirNm, execName = os.path.split(os.path.realpath(sys.argv[0]))
sys.path.append(os.path.realpath(os.path.join(dirNm, "../libexec")))
sys.path.append(os.path.realpath(os.path.join(dirNm, "../site")))

from XALTdb            import XALTdb
import xalt_rmap_util 
from xalt_site_pkg     import translate


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

def link_to_db(xalt, reverseMapT, linkT):

   try:
      conn   = xalt.connect()
      query  = "USE "+xalt.db()
      conn.query(query)
      query  = "SELECT uuid FROM xalt_link WHERE uuid='%s'" % linkT['uuid']
      conn.query(query)
      result = conn.store_result()
      if (result.num_rows() > 0):
        return

      build_epoch = float(linkT['build_epoch'])
      dateTimeStr = time.strftime("%Y-%m-%d %H:%M:%S",
                                  time.localtime(float(linkT['build_epoch'])))
      # It is unique: lets store this link record
      query = "INSERT into xalt_link VALUES (NULL,'%s','%s','%s','%s','%s','%s','%.2f','%d','%s') " % (
        linkT['uuid'],         linkT['hash_id'],         dateTimeStr,
        linkT['link_program'], linkT['build_user'],      linkT['build_syshost'],
        build_epoch,           int(linkT['exit_code']), linkT['exec_path'])
      conn.query(query)
      link_id = conn.insert_id()
      #print("link_id: ",link_id)

      load_xalt_objects(conn, linkT['linkA'], reverseMapT, linkT['build_syshost'],
                        "join_link_object", link_id)
      conn.commit()
      xalt.connect().close()

   except MySQLdb.Error, e:
      print ("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return None

def load_xalt_objects(conn, objA, reverseMapT, syshost, table, index):

  try:
    for entryA in objA:
      object_path  = entryA[0]
      hash_id      = entryA[1]
      if (hash_id == "unknown"):
        continue

      query = "SELECT obj_id FROM xalt_object WHERE hash_id='%s' AND object_path='%s' AND syshost='%s'" % (
        hash_id, object_path, syshost)
      
      conn.query(query)
      result = conn.store_result()
      if (result.num_rows() > 0):
        row    = result.fetch_row()
        obj_id = int(row[0][0])
      else:
        moduleName = obj2module(object_path, reverseMapT)
        obj_kind   = obj_type(object_path)

        query      = "INSERT into xalt_object VALUES (NULL,'%s','%s','%s',%s,NOW(),'%s') " % (
                    object_path, syshost, hash_id, moduleName, obj_kind)
        conn.query(query)
        obj_id   = conn.insert_id()
        #print("obj_id: ",obj_id, ", obj_kind: ", obj_kind,", path: ", object_path, "moduleName: ", moduleName)

      # Now link libraries to xalt_link record:
      query = "INSERT into %s VALUES (NULL,'%d','%d') " % (table, obj_id, index)
      conn.query(query)


  except MySQLdb.Error, e:
    print ("Error %d: %s" % (e.args[0], e.args[1]))
    sys.exit (1)


def run_to_db(xalt, reverseMapT, runT):

   nameA = [ 'num_cores', 'num_nodes', 'account', 'job_id', 'queue' ]
   try:
      conn   = xalt.connect()
      query  = "USE "+xalt.db()
      conn.query(query)

      translate(nameA, runT['envT'], runT['userT']);
      dateTimeStr = time.strftime("%Y-%m-%d %H:%M:%S",
                                  time.localtime(float(runT['userT']['start_time'])))
      uuid        = runT['xaltLinkT'].get('Build.UUID')
      if (uuid):
        uuid = "'" + uuid + "'"
      else:
        uuid = "NULL"

      #print( "Looking for run_uuid: ",runT['userT']['run_uuid'])

      query = "SELECT run_id FROM xalt_run WHERE run_uuid='%s'" % runT['userT']['run_uuid']
      conn.query(query)

      result = conn.store_result()
      if (result.num_rows() > 0):
        #print("found")
        row    = result.fetch_row()
        run_id = int(row[0][0])
        query  = "UPDATE xalt_run SET run_time='%.2f', end_time='%.2f' WHERE run_id='%d'" % (
          runT['userT']['run_time'], runT['userT']['end_time'], run_id)
        conn.query(query)
        return
      else:
        #print("not found")
        moduleName = obj2module(runT['userT']['exec_path'], reverseMapT)
        query  = "INSERT INTO xalt_run VALUES (NULL,'%s','%s','%s', '%s',%s,'%s', '%s','%s','%.2f', '%.2f','%.2f','%d', '%d','%d','%s', '%s','%s',%s,'%s') " % (
          runT['userT']['job_id'],      runT['userT']['run_uuid'],    dateTimeStr,
          runT['userT']['syshost'],     uuid,                         runT['hash_id'],
          runT['userT']['account'],     runT['userT']['exec_type'],   runT['userT']['start_time'],
          runT['userT']['end_time'],    runT['userT']['run_time'],    runT['userT']['num_cores'],
          runT['userT']['num_nodes'],   runT['userT']['num_threads'], runT['userT']['queue'],
          runT['userT']['user'],        runT['userT']['exec_path'],   moduleName,
          runT['userT']['cwd'])
        conn.query(query)
        run_id   = conn.insert_id()

      load_xalt_objects(conn, runT['libA'], reverseMapT, runT['userT']['syshost'],
                        "join_run_object", run_id) 

      # loop over env. vars.
      for key in runT['envT']:
        value = runT['envT'][key]
        query = "SELECT env_id FROM xalt_env_name WHERE env_name='%s'" % key
        conn.query(query)
        result = conn.store_result()
        if (result.num_rows() > 0):
          row    = result.fetch_row()
          env_id = int(row[0][0])
          found  = True
        else:
          query  = "INSERT INTO xalt_env_name VALUES(NULL, '%s')" % key
          conn.query(query)
          env_id = conn.insert_id()
          found  = False
        #print("env_id: ", env_id, ", found: ",found)

        query = "INSERT INTO join_run_env VALUES (NULL, '%d', '%d', '%s')" % (
          env_id, run_id, value)
        conn.query(query)
      conn.commit()
      xalt.connect().close()
        
   except MySQLdb.Error, e:
      print ("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return None




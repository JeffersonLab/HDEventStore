#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2005 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2005
#
"""Synchronize slave MySQL server up to certain position of the master one"""

import MySQLdb, time, string, sys
import esdb_auth, gen_util

def connect(dbHost,dbPort,dbName,userName="",userPass=""):
    """Establish connection to MySQL db"""
    try:
       userName, userPass = esdb_auth.authToESMySQL(dbHost,userName,userPass,"adminLogin")
       db = MySQLdb.connect(host=dbHost,port=dbPort,user=userName,passwd=userPass,db=dbName)
    except:
       print "WARNING: First attempt to connect to %s fail, try again"%dbHost
       time.sleep(60)
       try:
	  db = MySQLdb.connect(host=dbHost,port=dbPort,user=userName,passwd=userPass,db=dbName)
       except:
	  print "ERROR: Second attemp to connect to %s fail, please investigate"%dbHost
	  gen_util.printExcept()
	  sys.exit(1)
    return db
    
def getSlaveInfo(curS):
    """Retrieve slave information"""
    query   = "SHOW slave status";
    curS.execute(query)
    tup     = curS.fetchall()
    if tup:
       return tup[0]
    else:
       print "Error while connecting to slave server %s@%s"%(dbName,masterHost)
       
def syncToMaster(masterHost,slaveHost,dbName,userName="",userPass=""):
    """Try to synchronize slave to certain position in a master DB server"""
    splitName= string.split(masterHost,":")
    if len(splitName)==2:
       mHost = splitName[0]
       mPort = int(splitName[1])
    else:
       mHost = masterHost
       mPort = 3306
    dbMaster= connect(mHost,mPort,dbName,userName,userPass)
    curM    = dbMaster.cursor()
    query   = "show master status";
    curM.execute(query)
    tup     = curM.fetchall()
    if tup:
       logName = tup[0][0]
       logPos  = tup[0][1]
    else:
       print "Error while connecting to master server %s@%s"%(dbName,masterHost)
    splitName= string.split(slaveHost,":")
    if len(splitName)==2:
       sHost = splitName[0]
       sPort = int(splitName[1])
    else:
       sHost = masterHost
       sPort = 3306
    dbSlave = connect(sHost,sPort,dbName,userName,userPass)
    curS    = dbSlave.cursor()
    query="slave start"
    curS.execute(query)
    counter=0
    LIMIT=100
    while (1):
       if counter==LIMIT:
          print "ERROR: ESSync is give up after %s retries ###"%LIMIT
          break
       Master_Host, Master_User, Master_Port, Connect_retry, Master_Log_File, Read_Master_Log_Pos, \
       Relay_Log_File, Relay_Log_Pos, Relay_Master_Log_File, Slave_IO_Running, Slave_SQL_Running, \
       Replicate_do_db, Replicate_ignore_db, Last_errno, Last_error, Skip_counter, \
       Exec_master_log_pos, Relay_log_space = getSlaveInfo(curS)
       print "Master info: logName=%s logPosition=%s"%(logName,logPos)
       print "Slave  info: logName=%s logPosition=%s"%(Master_Log_File,Read_Master_Log_Pos)
       if not int(Last_errno):
          if logName==Master_Log_File and long(logPos)<=long(Read_Master_Log_Pos):
	     print "STOP slave server"
             query="slave stop"
             curS.execute(query)
	     infoTup = getSlaveInfo(curS)
	     print "Slave info:",infoTup
	     break
	  print "sleeping 5 sec"
	  infoTup = getSlaveInfo(curS)
	  print "Slave info:",infoTup
          time.sleep(5);
       else:
          print "Please resolve the following error on the slave"
	  print Last_error
	  sys.exit(1)
       counter+=1

#
# main
#
if __name__ == "__main__":
    MASTER='lnx151:3307'    ### update 
    SLAVE='lnx151:3306'     ### update
    ESDB='EventStore'
    syncToMaster(MASTER,SLAVE,ESDB)



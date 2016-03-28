#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""A simple convertor of MySQL<->SQLite databases for EventStore"""

import os, sys, re, string, socket
#import sqlite # pysqlite version 1
#import pysqlite2.dbapi2 as sqlite # pysqlite2
import sqlite3 as sqlite
import MySQLdb				# MySQL db module
import sql_util, esdb_auth, es_init

def convert(args):
    """convert module allow user to convert EventStore DB from MySQL into
    SQLite format and vice versa.
    
    Examples:	  
      - SQLite into MySQL
        - convert.py -in sqlite sqlite.db -out mysql lnx151 -esdb EventStoreTMP
      - MySQL into SQLite:
        - convert.py -out sqlite sqlite.db -in mysql lnx151 -esdb EventStoreTMP
    """
    usage="""\n
    Usage: convert.py -in   <sqlite or mysql> <fileName or hostName>
		      -out  <sqlite or mysql> <fileName or hostName>
		      -esdb <EventStoreDBName>
	    
    Examples:	  
    to convert SQLite into MySQL
	   convert.py -in sqlite sqlite.db -out mysql lnx151 -esdb EventStoreTMP
    to convert MySQL into SQLite:
	   convert.py -out sqlite sqlite.db -in mysql lnx151 -esdb EventStoreTMP
		      """

    sqliteFile= "sqlite.db"
    esdb      = "EventStoreTMP"
    readIn    = ""
    readInDB  = ""
    writeOut  = ""
    writeOutDB= ""
    x         = 1
    inOut     = 0
    while x < len(args):
	try:
	    tryDB=args[x]
	    if args[x] == "-in" : 
	       readIn   = args[x+1]
	       readInDB = args[x+2]
	       inOut+=1
	    if args[x] == "-out" : 
	       writeOut   = args[x+1]
	       writeOutDB = args[x+2]
	       inOut+=1
	    if args[x] == "-esdb" : 
	       esdb = args[x+1]
	    x = x+1
	except:
	    print usage
	    sys.exit()

    if inOut!=2:
       print usage
       sys.exit()

    writeSQLite = 0
    whichDB     = "mysql"
    sqliteFile  = readInDB
    mysqlHost   = writeOutDB
    if string.find(string.lower(writeOut),"sqlite")!=-1: 
       writeSQLite = 1
       whichDB = "sqlite"
       sqliteFile = writeOutDB
       mysqlHost = readInDB
       
# write out what we're doing
    print "Start conversion (%s): %s -> %s"%(esdb,readInDB,writeOutDB)
    sys.__stdout__.flush()

# make connection to EventStoreDB in MySQL
    userMySQL, passwordMySQL = esdb_auth.authToESMySQL(mysqlHost)
    tup = string.split(mysqlHost,":")
    if len(tup)==2:
       iHost = tup[0]
       if re.search("[a-zA-Z]",tup[1]):
          raise "Fail to decode port number from '%s'"%tup[1]
       iPort = int(tup[1])
    else:
       iHost = tup[0]
       iPort = 3306
    mysqldb  = MySQLdb.connect(host=iHost,port=iPort,user=userMySQL,passwd=passwordMySQL,db=esdb)

# remove sqliteFile if necessary
    if writeSQLite:
       if os.path.isfile(sqliteFile):
	  os.remove(sqliteFile)
	    
# make connection to EventStoreDB in SQLite
    sqlitedb = sqlite.connect(sqliteFile)

# make a choice from which DB we will read and to which we will write
    if writeSQLite:
       dbIn      = mysqldb
       dbOut     = sqlitedb
       dbTypeIn  = "mysql"
       dbTypeOut = "sqlite"
    else:
       dbIn      = sqlitedb
       dbOut     = mysqldb
       dbTypeIn  = "sqlite"
       dbTypeOut = "mysql"

# Drop/create all tables
    dbLog   = open('esdb.log','w')
    sqlUtil = sql_util.SQLUtil(dbOut,dbTypeOut,dbLog)
    sqlUtil.createTables()

# get table names from DB
#    esInit = es_init.ESInit(dbIn,dbTypeIn,dbLog)
#    tNames = esInit.dbNames
    tNames = sqlUtil.getTableNames()
    maxLength = 0
    for esTable in tNames:
        if maxLength<len(esTable):
	   maxLength=len(esTable)
    maxLength+=1

    cursorIn = dbIn.cursor()
    cursorOut= dbOut.cursor()
    for esTable in tNames:
        empty = "."*(maxLength-len(esTable))
	msg   = esTable+empty
	print msg,
	sys.__stdout__.flush()
	try:
	   query = "SELECT * FROM %s"%esTable
	   cursorIn.execute(query)
	   tup = cursorIn.fetchall()
	   for x in tup:
	       query="INSERT INTO %s VALUES ( "%esTable
	       for idx in xrange(0,len(x)):
		   query+="'%s'"%x[idx]
		   if idx!=len(x)-1:
		      query+=","
	       query+=" ) "
	       cursorOut.execute(query)
	   print " [done]"
	except:
	   print " [fail]"
	   sys.exit(1)

# In the case of SQLite DB we need to invoke commit action
    if writeSQLite:
       sqlitedb.commit()

#
# main
#
if __name__ == "__main__":
    convert(sys.argv)

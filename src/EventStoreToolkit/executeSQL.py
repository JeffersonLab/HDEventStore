#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""A stand-alone script which executes SQL statements from ASCII file"""

import sys,os,string
import MySQLdb
import sqlite3 as sqlite
#import sqlite # pysqlite version 1
#import pysqlite2.dbapi2 as sqlite # pysqlite2

#
# main
#
if __name__ == "__main__":
    usage="""\n
    Usage: executeSQL.py -file <sqlFile> -sqlite <sqlite DB file>\n""" 

    sqliteFile = "sqlite.db"
    sqlFile    = ""
    useSQLite  = 0
    x          = 1
    while x < len(sys.argv):
        try:
            tryDB=sys.argv[x]
            if sys.argv[x] == "-sqlite" : 
               sqliteFile  = sys.argv[x+1]
               useSQLite   = 1
            if sys.argv[x] == "-file" : 
               sqlFile = sys.argv[x+1]
            x = x+1
        except:
            print usage
            sys.exit()

    if not sqlFile:
       print usage
       sys.exit()

    if useSQLite:
       db = sqlite.connect(sqliteFile)
    else:
       db = MySQLdb.connect('localhost','root','angarsk','EventStoreDB')
    cu = db.cursor()

    file = open(sqlFile,'r')

    print "Execute:"
    for line in file.readlines():
        act = string.replace(line,'\n','')
        if len(act)<3: continue
        print act
        try:
           res = cu.execute(act)
           for row in res:
               print row
        except:
           print "The following SQL statement failed:",act
        db.commit()
            

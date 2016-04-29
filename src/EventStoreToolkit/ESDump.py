#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""ESDumpManager retrieve information from EventStore tables and provides methods to access it."""

import os, sys, string, time
import es_init
from es_init import ESInit, checkArg

# include python profiler only if python version greater then 2.3
if es_init.checkPythonVersion("2.3"):
   import hotshot			# Python profiler
   import hotshot.stats			# profiler statistics

class ESDumpManager(ESInit):
    """A dump manager provide utilities to print various information about
    EventStore DB"""
    def __init__(self,db, dbType, logFile):
        ESInit.__init__(self, db, dbType, logFile)
    def dumpTableSchema(self,tableName):
        """Prints table schema. It uses sql_util module to
	do the job."""
        print self.getTableSchema(tableName)
        return self.ok
    def dumpInfo(self,timeStamp):
	"""Dump EventStore DB content in user readable format. It uses sql_util module to
	do the job."""
	self.printESInfo(timeStamp)
	return self.ok
    def dumpRuns(self,minRun,maxRun):
	"""Prints all available runs in given run range. It uses sql_util module to
	do the job."""
	self.printRuns(minRun,maxRun)
	return self.ok
    def findFile(self,searchRun,time=0):
	"""Find file(s) associated with given run. It uses sql_util module to
	do the job."""
	self.findFileForRun(searchRun,time)
	return self.ok
    def dumpShowDepend(self,childName):
	"""Show all dependencies for given data version name. It uses sql_util module to
	do the job."""
	self.showDepend(childName)
	return self.ok
    def dumpTable(self,dbTable):
        """Prints table content. It uses sql_util module to do the job."""
	if dbTable=="all":
	   for x in self.dbNames:
	       self.printDBContent(x)
	else:
	   self.printDBContent(dbTable)
	return self.ok
#
# main
#
if __name__ == "__main__":
	localOpt=["[ -info ] [ -runList <minRun> <maxRun> ] [ -time <YYYYDDMM> ]"]
	localOpt.append("[ -findFileForRun <run> ] [ -showDepend <dataVersionName> ]")
	localOpt.append("[ -dbTable <dbTable> ] [ -tableSchema <tableName>]")
	usage=es_init.helpMsg("ESDump",localOpt)
	usageDescription="""
Option description:
	"""

	examples   = es_init.ESExamples()
	userCommand="ESDump.py"
	optList, dictOpt = es_init.ESOptions(userCommand,sys.argv,usage,usageDescription)
	dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
	historyFile,logFile,verbose,profile             = optList[1]
	userCommand                                     = optList[2]

	usageDescription="""
ESDump provides usefull information about data store in EventStore.
Default ESDump print content of group EventStore at local time
(MySQL server at Cornell)
	"""

	timeStamp= time.strftime("%Y%m%d",time.localtime())
	dbTable  = "all"
	tableSchema=""
	childName=""
	info     = 1
	runs     = 0
	find     = 0
	x        = 1
	while x < len(sys.argv):
	    try:
		tryDB=sys.argv[x]
		if sys.argv[x] == "-showDepend" : 
		   childName = sys.argv[x+1]
		   info = 0
		   runs = 0
		   checkArg([childName])
		   x+=2
		   continue
		if sys.argv[x] == "-dbTable" : 
		   info = 0
		   dbTable = sys.argv[x+1]
		   checkArg([dbTable])
		   x+=2
		   continue
		if sys.argv[x] == "-tableSchema" : 
		   info = 0
		   tableSchema = sys.argv[x+1]
		   checkArg([tableSchema])
		   x+=2
		   continue
		if sys.argv[x] == "-runList" : 
		   info = 0
		   runs = 1
		   minRun = sys.argv[x+1]
		   maxRun = sys.argv[x+2]
		   checkArg([minRun,maxRun])
		   x+=3
		   continue
		if sys.argv[x] == "-findFileForRun" : 
		   info = 0
		   find = 1
		   searchRun = sys.argv[x+1]
		   checkArg([searchRun])
		   x+=2
		   continue
		if sys.argv[x] == "-info" : 
		   info = 1
		   x+=1
		   continue
		if sys.argv[x] == "-time" : 
		   timeStamp = sys.argv[x+1]
		   checkArg([timeStamp])
		   x+=2
		   continue
	        # if we reach here, that means we found unkown option
		if dictOpt.has_key(sys.argv[x]):
		   x+=dictOpt[sys.argv[x]]
		else:
		   print "Option '%s' is not allowed"%sys.argv[x]
		   raise
	    except:
		sys.exit(1)

	# connect to DB
	outputLog, globalLog = es_init.ESOutputLog(logFile) 
	db, dbType           = es_init.ESDBConnector(dbHost,dbName,userName,userPass,'',dbPort,dbSocket)
	msg = "-"*(len(dbName)+3+len(dbHost))
	print " %s "%msg
	print "| %s@%s |"%(dbName,dbHost)
	print " %s "%msg
	print

	# create instance of ESDumpManager class
	mydb = ESDumpManager(db,dbType,outputLog)

	if info:
	   status=mydb.dumpInfo(timeStamp)
	elif runs:
	   status=mydb.dumpRuns(minRun,maxRun)
	elif find:
	   status=mydb.findFile(searchRun,timeStamp)
	elif childName:
	   status=mydb.dumpShowDepend(childName)
	elif tableSchema:
	   status=mydb.dumpTableSchema(tableSchema)
	else:
	   status=mydb.dumpTable(dbTable)
	if status==1:
	   os.remove(outputLog.name)

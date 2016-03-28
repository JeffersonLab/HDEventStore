#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2005
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2005
"""This tool designed to help merge multiple data version names into new graphid
Algorithm: upon request to merge multiple svName's:
  - resolve their svId's
  - add entry into GraphPath newGraphId->svid1, newGraphId->svid2, etc.
  - add entry into Version with all run-ranges from gid1, gid2, etc.
  - copy entries from KeyFile with gid1->newGraphId, gid2->newGraphId
  - copy entries from Location with gid1->newGraphId, gid2->newGraphId
"""

import os, sys, string, time
import es_init
from es_init import ESInit, checkArg

# include python profiler only if python version greater then 2.3
if es_init.checkPythonVersion("2.3"):
   import hotshot			# Python profiler
   import hotshot.stats			# profiler statistics

class ESMergeSVManager(ESInit):
    """ESMergeSVManager is in charge of merging multiple svNames into new graphid"""
    def __init__(self,db, dbType, logFile):
        ESInit.__init__(self, db, dbType, logFile)
	self.verbose = 0
	if dbType=="sqlite":
	   self.addToQuery=""
	elif dbType=="mysql":
	   self.addToQuery=" FOR UPDATE"
	   
    def setTime(self,timeS):
        """Set time stamp to be used"""
        self.time=timeS
	
    def setGrade(self,grade):
        """Set grade to be used"""
        self.grade=grade
	
    def merge(self,svList):
        """Main routine to merge and update all tables using given data version list:
           - resolve their svId's
           - add entry into GraphPath newGraphId->svid1, newGraphId->svid2, etc.
           - add entry into Version with all run-ranges from gid1, gid2, etc.
           - copy entries from KeyFile with gid1->newGraphId, gid2->newGraphId
           - copy entries from Location with gid1->newGraphId, gid2->newGraphId
        """
        newgid, gidList = self.updateVersion(svList)
        self.updateKeyFile(newgid, gidList)
        self.updateLocation(newgid, gidList)
	return self.ok
    
    def updateVersion(self,svList):
        """Update Version and GraphPath tables"""
	gidDict  = {}
        for svName in svList:
            query="""SELECT graphid FROM SpecificVersion,GraphPath where svName='%s' AND
	             SpecificVersion.svid=GraphPath.svid"""%svName
            tup  = self.fetchAll(query)
	    for item in tup:
	        gid   = tup[0][0]
	        query = "SELECT minRunNumber,maxRunNumber FROM Version where graphid='%s'"%gid
		data  = self.fetchAll(query)
	        runList  = []
	        for elem in data:
		    minR = elem[0]
		    maxR = elem[1]
		    if not runList.count((minR,maxR)):
		       runList.append((minR,maxR))
	        gidDict[gid]=runList

	# update Version and GraphPath tables
	self.writeToLog("updateVersion")
        self.startTxn("updateVersion")
        query = "SELECT MAX(graphid) FROM Version"+self.addToQuery
	tup   = self.fetchOne(query)
	newgid= tup[0]+1
	for gid in gidDict.keys():
	    # update GraphPath table
	    query   = "INSERT INTO GraphPath VALUES ('%s','%s')"%(newgid,gid)
	    self.updateDBAndLog(query)
            # update Version table
	    runList = gidDict[gid]
	    for item in runList:
	        minR= item[0]
		maxR= item[1]
		if self.dbType=="sqlite":
		   query = """INSERT INTO Version 
			      (id,grade,timeStamp,minRunNumber,maxRunNumber,graphid,state) 
			      VALUES (NULL,'%s','%s','%s','%s','%s','active')
			      """%(self.grade,self.time,minR,maxR,newgid)
		else:
		   query = """INSERT INTO Version 
			      (id,grade,timeStamp,minRunNumber,maxRunNumber,graphid,state) 
			      VALUES ('%s','%s','%s','%s','%s','active')
			      """%(self.grade,self.time,minR,maxR,newgid)
		self.updateDBAndLog(query)
	self.endTxn("updateVersion")
	return (newgid,gidDict.keys())
    
    def updateKeyFile(self,newgid,gidList):
        """Update KeyFile table"""
	self.writeToLog("updateKeyFile")
	self.startTxn("updateKeyFile")
        for gid in gidList:
            query = "SELECT view,run,uid,keyFileId from KeyFile WHERE graphid='%s'"%gid
	    tup   = self.fetchAll(query)
	    for item in tup:
		view  = item[0]
		run   = item[1]
		uid   = item[2]
		kid   = item[3]
		query ="INSERT INTO KeyFile VALUES('%s','%s','%s','%s','%s')"%(newgid,view,run,uid,kid)
		self.updateDBAndLog(query)
	self.endTxn("updateKeyFile")
	    
    def updateLocation(self,newgid,gidList):
        """Update Location table"""
	self.writeToLog("updateLocation")
	self.startTxn("updateLocation")
        for gid in gidList:
            query = "SELECT run,uid,locationFileId from Location WHERE graphid='%s'"%gid
	    tup   = self.fetchAll(query)
	    for item in tup:
		run   = item[0]
		uid   = item[1]
		lid   = item[2]
		query = """INSERT INTO Location (graphid,run,uid,locationFileId)
			   VALUES ('%s','%s','%s','%s')"""%(newgid,run,uid,lid)
		self.updateDBAndLog(query)
	self.endTxn("updateLocation")
        
#
# main
#
if __name__ == "__main__":

	# initialize user options
	localOpt=["[ -grade <gradeName> ]  [ -time <timeStamp> ]"]
	localOpt.append("[ -listOfNames <name1,name2> ]")
	localOpt.append("")
	
	usage=es_init.helpMsg("ESMergeSVManager",localOpt)
	usageDescription="""
Option description (required options marked with '*'):
*       -grade:      specifies the grade, e.g. "physics", "mc-unchecked"
                     you need to provide two of them, 'old' from which you're reading
		     and 'new' to which you're moving
*       -time:       specifies the timeStamp, e.g. 20090227
                     you may provide one or two of them, 'old' from which you're reading
		     and 'new' to which you're moving
*	-listOfNames specify a list of data version names to be merged together under new graphid.
	"""
	examples="""
ESMergeSVManager -grade physics-unchecked -time 20090909 -listOfNames svName1 svName2
	"""
	userCommand="ESMergeSVManager.py"
	
	# get EventStore option list
	optList,dictOpt = es_init.ESOptions(userCommand,sys.argv,usage,usageDescription,examples)
	dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
	historyFile,logFile,verbose,profile             = optList[1]
	userCommand                                     = optList[2]+" "
	db, dbType           = es_init.ESDBConnector(dbHost,dbName,userName,userPass)
	outputLog, globalLog = es_init.ESOutputLog(logFile) 

	iGrade =""
	iTime  =""
	listOfSVNames = []
	x = 1
	while x < len(sys.argv):
	    try:
		if sys.argv[x]=="-listOfNames":
		   x+=1
		   while(sys.argv[x][0]!="-"):
		      newArg = sys.argv[x]
		      listOfSVNames.append(sys.argv[x])
		      x+=1
		      if len(sys.argv)==x:
			 break
		   checkArg(listOfSVNames)
		   continue
		if sys.argv[x]=="-grade":
		   iGrade = sys.argv[x+1]
		   checkArg([iGrade])
		   x+=2
		   continue
		if sys.argv[x]=="-timeStamp" or sys.argv[x]=="-time":
		   iTime = sys.argv[x+1]
		   checkArg([iTime])
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

	if not iGrade:
	   print "You need to provide a new grade"
	   print usage
	   sys.exit(1)
	 
	if not iTime:
	   print "You need to provide a time stamp"
	   print usage
	   sys.exit(1)
		
	pid       = "%s"%os.getpid()
	localtime = "%s"%time.strftime("%H:%M:%S",time.localtime())
	outputLog.write("\n%s %s ###### %s initialization is completed"%(pid,localtime,dbType))

	# create instance of ESManager class
	manager = ESMergeSVManager(db,dbType,outputLog)
	manager.setTime(iTime)
	manager.setGrade(iGrade)
	manager.setVerboseLevel(verbose)

	# update DB using transaction
	if profile:
	   print "Run in profiler mode"
	   if majorNumber>=2 and minorNumber>=3:
	      profiler = hotshot.Profile("profile.dat")
	      profiler.run("status=manager.merge(listOfSVNames)")
	      profiler.close()
	      stats = hotshot.stats.load("profile.dat")
	      stats.sort_stats('time', 'calls')
	      stats.print_stats()
	else:
	   status = manager.merge(listOfSVNames)

	rStatus = es_init.ESOutput(status,userCommand,historyFile,outputLog,globalLog)
	sys.exit(rStatus)

	 

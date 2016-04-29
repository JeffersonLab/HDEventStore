#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004, 2005 
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2005
#
"""ESDeleteManager handles the case of removing data from EventStore.
data files are marked as orphans and associative key/location files are deleted."""

import os, sys, string, time, glob, stat, es_init, file_util, sql_util
from es_init import ESInit, checkArg

# include python profiler only if python version greater then 2.3
if es_init.checkPythonVersion("2.3"):
   import hotshot			# Python profiler
   import hotshot.stats			# profiler statistics

class ESDeleteManager(ESInit):
    """ESDeleteManager responsible for deleting entries in EventStore"""
    def __init__(self,db, dbType, logFile):
        ESInit.__init__(self, db, dbType, logFile)
    def formDict(self,tup):
        """Form a dictionary dict[id]=runList where id is file id"""
        dict = {}
	for x in tup:
	    run = int(x[0])
	    id  = int(x[1])
	    if dict.has_key(id): 
	       runList = dict[id]
	       if not runList.count(run): runList.append(run)
	       dict[id] = runList
	    else:
	       dict[id] = [run]
	return dict
    def removeElementFromDict(self,dict,minR,maxR):
        """Remove element from dict[id]=runList"""
	for id in dict.keys():
	    runList = dict[id]
	    for run in runList:
		if run>=minR and run<=maxR:
		   del dict[id]
	return dict
    def doDelete(self,dict,table):
        """Remove entries in KeyFile and Location tables. When it's done remove physically file from the system."""
        if table=="KeyFile": field="keyFileId"
        elif table=="Location": field="locationFileId"
	try:
	   print "Delete files:"
	   for id in dict.keys():
	       # remove entry from KeyFile
	       query = "DELETE FROM "+table+" WHERE "+field+"='%s'"%id
	       self.updateDBAndLog(query)
	       query = "SELECT fileName FROM FileID WHERE fileId='%s'"%id
	       tup = self.fetchOne(query)
	       fileName = tup[0]
	       if os.path.isfile(fileName):
	          print fileName
		  os.remove(fileName)
	except:
	   print "fail doing deletion of files"
	   return self.error
	return self.ok
    def deleteGrade(self,grade,timeS):
        """'Delete' given grade/timeS in EventStore. Information about such grade is
	still available but its state is marked as removed and its information
        moved into OrphanFile table."""
	
        self.writeToLog("deteleGrade")
	query="""SELECT graphid,minRunNumber,maxRunNumber 
	FROM Version WHERE grade='%s' AND timeStamp='%s'
	"""%(grade,timeS)
	tup = self.fetchAll(query)
	graphidDict = {}
	graphidRuns = {}
	fDict = {}
	for item in tup:
	    # collect all runs/(key,loc)Ids to be candidates for removal
	    graphid = item[0]
	    minR = item[1]
	    maxR = item[2]
	    query="""SELECT run,locationFileId FROM Location 
	    WHERE graphid='%s' AND run>='%s' AND run<='%s'"""%(graphid,minR,maxR)
	    data = self.fetchAll(query)
	    # let's check if locationFileId is used in another (parent) grade
	    dataToGo = []
	    for d in data:
	        run  = d[0]
		id   = d[1]
		query="SELECT graphid FROM Location WHERE locationFileId='%s'"%id
		tup  = self.fetchAll(query)
	        if len(tup)==1:
		   dataToGo.append(d)
	    locIdDict = self.formDict(dataToGo)
	    query="""SELECT run,keyFileId FROM KeyFile 
	    WHERE graphid='%s' AND run>='%s' AND run<='%s'"""%(graphid,minR,maxR)
	    data = self.fetchAll(query)
	    # let's check if keyFileId is used in another (parent) grade
	    dataToGo = []
	    for d in data:
	        run  = d[0]
		id   = d[1]
		query="SELECT graphid FROM KeyFile WHERE keyFileId='%s'"%id
		tup  = self.fetchAll(query)
	        if len(tup)==1:
		   dataToGo.append(d)
	    keyIdDict = self.formDict(dataToGo)
	    graphidDict[graphid]=(keyIdDict,locIdDict)
	    graphidRuns[graphid]=(minR,maxR)
	    
	    # select other run ranges from grades which share the same graphid
#            query="""SELECT minRunNumber,maxRunNumber
#                FROM Version WHERE graphid='%s' AND grade!='%s'
#                AND timeStamp!='%s'"""%(graphid,grade,timeS)
#            tup = self.fetchAll(query)
#            for item in tup:
#                minR = int(item[0])
#                maxR = int(item[1])
#                keyIdDict=self.removeElementFromDict(graphidDict[graphid][0],minR,maxR)
#                locIdDict=self.removeElementFromDict(graphidDict[graphid][1],minR,maxR)
	    fDict[graphid] = (keyIdDict,locIdDict)
	    
#        debug = fDict[graphid]
#        print debug[1]
	for graphid in fDict.keys():
	    minR = graphidRuns[graphid][0]
	    maxR = graphidRuns[graphid][1]
	    # update Version
	    query="""UPDATE Version SET state='removed'
		     WHERE grade='%s' AND timeStamp='%s'
		     AND minRunNumber='%s' AND maxRunNumber='%s'
		     AND graphid='%s'"""%(grade,timeS,minR,maxR,graphid)
	    self.startTxn()
	    self.updateDBAndLog(query)
	    self.endTxn()
	    # delete all key files
	    keyIdDict = fDict[graphid][0]
	    if self.doDelete(keyIdDict,"KeyFile")!=self.ok:
	       return self.error
	    # scan location file(s), get back data file Id and
	    # mark them in OrphanFileID
	    locIdDict = fDict[graphid][1]
	    for locId in locIdDict.keys():
	        query = "SELECT fileName FROM FileID WHERE fileId='%s'"%locId
	        tup = self.fetchOne(query)
	        if not tup: return self.error
		fileName = tup[0]
	        content  = file_util.locationFileParser(fileName)
		fileIdList = content[1]
		for id in fileIdList:
		    try:
		       mTime = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
		       user  = os.environ['USER']
		       query = "INSERT INTO OrphanFileID (id,dateTime,user) VALUES ('%s','%s','%s')"%(id,mTime,user)
		       self.startTxn()
		       self.updateDBAndLog(query)
		       self.endTxn()
		    except:
		       pass # we insert duplicate it's fine
	    # delete all location files
	    if self.doDelete(locIdDict,"Location")!=self.ok:
	       return self.error
	return self.ok
		
#
# main
#
if __name__ == "__main__":
	localOpt=["[ -delete <grade> <timeStamp> ]"]
	usage=es_init.helpMsg("ESDelete",localOpt)

	usageDescription="""
Option description:
	"""

	examples   = es_init.ESExamples()
	userCommand="ESDelete.py"
	optList, dictOpt = es_init.ESOptions(userCommand,sys.argv,usage,usageDescription)
	dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
	historyFile,logFile,verbose,profile             = optList[1]
	userCommand                                     = optList[2]
	
	# default values
	delGrade = ""
	delTime  = ""
	# parse the rest of the options and form user's command
	x = 1
	doNotRead  = 0
	while x < len(sys.argv):
          try:
	     if sys.argv[x] == "-delete" :
	        delGrade = sys.argv[x+1]
	        delTime  = int(sys.argv[x+2])
	        checkArg([delGrade,delTime])
	        x+=3
		continue
	     # if we reach here, that means we found unkown option
	     if dictOpt.has_key(sys.argv[x]):
		x+=dictOpt[sys.argv[x]]
	     else:
		print "Option '%s' is not allowed"%sys.argv[x]
		raise
          except:
             sys.exit(1)
	
	# perform local checks
	
	# connect to MySQL EventStoreDB
	outputLog, globalLog = es_init.ESOutputLog(logFile) 
	db, dbType           = es_init.ESDBConnector(dbHost,dbName,userName,userPass,'',dbPort,dbSocket)
	pid                  = "%s"%os.getpid()
	localtime            = "%s"%time.strftime("%H:%M:%S",time.localtime())
	outputLog.write("\n%s %s ###### %s initialization is completed"%(pid,localtime,dbType))

	# create instance of ESManager class
	mydb = ESDeleteManager(db,dbType,outputLog)

	# update DB using transaction
	if profile:
	   print "Run in profiler mode"
	   if es_init.checkPythonVersion("2.3"):
	      profiler = hotshot.Profile("profile.dat")
	      profiler.run("status=mydb.deleteGrade(delGrade,delTime)")
	      profiler.close()
	      stats = hotshot.stats.load("profile.dat")
	      stats.sort_stats('time', 'calls')
	      stats.print_stats()
	else:
	   status = mydb.deleteGrade(delGrade,delTime)

	rStatus = es_init.ESOutput(status,userCommand,historyFile,outputLog,globalLog)
	sys.exit(rStatus)


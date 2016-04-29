#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004, 2005 
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
# This utility allows add additional entries into VersionDB of EventStore.
# You provide name of EventStore DB, new administrative version and time stamp
# as well as ASCII file of runs you want either exclude or cover. In last case
# you SHOULD add additional parameters 'duplicate' to command line.
"""ESVersionManager is in charge of moving grades within EventStore"""

import os, sys, string, re, time
import esdb_auth, os_path_util, es_init, gen_util
from es_init import ESInit, checkArg

# include python profiler only if python version greater then 2.3
if es_init.checkPythonVersion("2.3"):
   import hotshot			# Python profiler
   import hotshot.stats			# profiler statistics

class ESVersionManager(ESInit):
    """ESVersionManager is in charge of moving grades within EventStore"""
    def __init__(self,db, dbType, logFile):
        ESInit.__init__(self, db, dbType, logFile)
	self.verbose = 0
        self.minR = minR
	self.maxR = maxR
	self.svName = svName
	if dbType=="sqlite":
	   self.addToQuery=""
	elif dbType=="mysql":
	   self.addToQuery=" FOR UPDATE"
	self.badRunList=[]
	self.goodRunList=[]
    def setMinRun(self,minR):
        """Set lower run range bound to be used"""
        self.minR = minR
    def setMaxRun(self,maxR):
        """Set upper run range bound to be used"""
        self.maxR = maxR
    def setSVName(self,svName):
        """Set data version name to be used"""
        self.svName = svName
    def setExcludeRunList(self,runList):
        """Set list of runs to be excluded"""
	runList.sort()
        self.badRunList = runList
    def setGoodRunList(self,runList):
        """Set list of runs to be excluded"""
	runList.sort()
        self.goodRunList = runList
	
    def findLatestTimeStamp(self,iGrade):
        """Search for latest timeStamp in Version table, it will assign a day ahead
	for non existing grade."""
	query = "SELECT MAX(timeStamp) FROM Version WHERE grade='%s'"%iGrade
	if self.verbose:
	   print query
	self.updateDBAndLog(query)
	tup = self.fetchOne(query)
	if tup and tup[0]!=None:
	   tStamp= tup[0]
	else:
	   # no timeStamp found for given grade, we'll use one day ahead
	   tStamp = gen_util.dayAhead()
	   print "No valid timeStamp found for '%s'. We'll use %s"%(iGrade,tStamp)
	if self.verbose:
	   print "Use timeStamp:",tStamp
	return tStamp
	
    def formQuery(self,iGrade,iTime):
        """Form a query for given grade/time to lookup in Version table"""
	if not iTime: 
           # time was not specified, means that we need to find out the greatest
	   # timeStamp for given grade
	   tStamp=self.findLatestTimeStamp(iGrade)
	   # form query
	   query = "SELECT * FROM Version WHERE grade='%s' AND timeStamp='%s'"%(iGrade,tStamp)
	   try:
	      tempQuery = "SELECT graphid FROM SpecificVersion,GraphPath WHERE svName='%s' AND SpecificVersion.svid=GraphPath.svid ORDER BY graphid DESC"%svName
	      if self.verbose:
	         print tempQuery
	      self.updateDBAndLog(tempQuery)
	      tup = self.fetchOne(tempQuery)
	      graphid = tup[0]
	      query+=" AND graphid='%s'"%graphid
	   except:
	      print "Error while lookup graphid"
	      raise
	   if self.verbose:
	      print query
	   return (query,tStamp)
	
	query = """
	SELECT * FROM Version WHERE grade='%s' AND timeStamp='%s' """%(iGrade,iTime)
	# we can be more specific in the case when minR/maxR are known
	if self.minR and self.maxR:
	   query+=" AND minRunNumber>=%s and maxRunNumber<=%s"%(self.minR,self.maxR)
	if self.svName:
	   try:
	      tempQuery = "SELECT graphid FROM SpecificVersion,GraphPath WHERE svName='%s' AND SpecificVersion.svid=GraphPath.svid ORDER BY graphid DESC"%svName
	      if self.verbose:
	         print tempQuery
	      self.updateDBAndLog(tempQuery)
	      tup = self.fetchOne(tempQuery)
	      if tup:
	         graphid = tup[0]
	         query+=" AND graphid='%s'"%graphid
	   except:
	      print "Error while lookup graphid"
	      raise
	if self.verbose:
	   print query
	return (query,iTime)
	
    def formNewRunList(self,iGrade="",iTime=""):
        """Form a new run-range list for given grade/timeStamp from badRunList or 
	use goodRunList for make it"""
	newRunRanges = []
	# we need to find out minR/maxR for given svName
	query="""
	SELECT MIN(minRunNumber),MAX(maxRunNumber) 
	FROM Version,SpecificVersion,GraphPath
	WHERE Version.graphid=GraphPath.graphid 
	AND SpecificVersion.svid=GraphPath.svid
	AND SpecificVersion.svName='%s' 
	AND Version.grade='%s' AND Version.timeStamp='%s'
	"""%(self.svName,iGrade,iTime)
	self.updateDBAndLog(query)
	tup = self.fetchAll(query)
	if not tup:
	   return newRunRanges
	minRunInDB=tup[0][0]
	maxRunInDB=tup[0][1]
        if self.badRunList:
	   minRun=minRunInDB
	   maxRun=maxRunInDB
	   if not minRun or not maxRun:
	      return newRunRanges
	   if self.verbose:
	      print query
	      print "result",minRun,maxRun
	   
	   # form a runList within run range for given grade/timeStamp
	   runList = []
	   for run in self.badRunList:
	       if int(minRun)<=int(run) and int(run)<=int(maxRun):
		  runList.append(run)

	   # form a list of run range pairs
	   for run in runList:
	       # check low bound and shifted every time
	       if run==minRun:
		  minRun=run+1
		  continue
	       newRunRanges.append((minRun,run-1))
	       minRun=run+1
	   # finally check upper bound and update a list if necessary
	   if minRun!=maxRun and minRun<maxRun:
	      newRunRanges.append((minRun,maxRun))

	   if not len(newRunRanges):
	      newRunRanges.append((minRun,maxRun))
	      
	if self.goodRunList: 
	   # query last available run in DB
	   minRun = self.goodRunList[0]
	   maxRun = minRun
	   if not maxRunInDB:
	      maxRunInDB=0
	   for run in self.goodRunList:
	       if int(run)-int(maxRun)>1:
	          if int(maxRun)>int(maxRunInDB):
		     maxRun=maxRunInDB
		     break
                  newRunRanges.append((minRun,maxRun))
   		  minRun = run
		  maxRun = run
   	          continue
               maxRun=run
	   newRunRanges.append((minRun,maxRun))

	if self.verbose:
	   print "newRunRanges",newRunRanges
	return newRunRanges

    def checkRunsInDB(self,grade,tStamp):
        """Check if new list overlap with a list of run-ranges in DB"""
	if not tStamp:
	   # find out latest timeStamp for that grade
	   tStamp = self.findLatestTimeStamp(grade)
	# get existing run list from DB
	newRunList = self.formNewRunList(grade,tStamp)
	query="""
	SELECT minRunNumber,maxRunNumber FROM Version,GraphPath,SpecificVersion 
	WHERE grade='%s' AND timeStamp=%s AND GraphPath.graphid=Version.graphid
	AND SpecificVersion.svid=GraphPath.svid AND SpecificVersion.svName='%s'
	"""%(grade,tStamp,self.svName)
	self.updateDBAndLog(query)
	tup = self.fetchAll(query)
	dbGoodRunList=[]
	for item in tup:
	    dbGoodRunList.append(item)
	dbGoodRunList.sort()
	if not dbGoodRunList or not newRunList:
	   return
	diff = self.diffLists(newRunList,dbGoodRunList)
	maxRunInES = int(dbGoodRunList[-1][1])
	if diff:
	   # check if found difference is still valid for appending to ES
	   if es_init.checkPythonVersion("2.4"):
	      runRange = diff.pop() # here 'diff' is a set of run ranges
	   else:
	      runRange = diff[0]    # here 'diff' is a list of run ranges
	   minRunInNew = int(runRange[0])
	   if minRunInNew<=maxRunInES:
	      print "Found at least one run-range which overlap with run-ranges in ES:"
	      print runRange
	      print "max run in ES is %s, min run in provided list is %s"%(maxRunInES,minRunInNew)
	      print "To resolve this issue you need to assign a new time stamp for"
	      print "your injection."
	      sys.exit(1)

    def diffLists(self,list1,list2):	
        """Compare two list of run-ranges. Order lists and look first if 
	for intersection between them, if found show difference between list1 and list2"""
	if list1==list2: return 0
	if list1!=list2:
       	   if list1[-1][1]<list2[0][0] or list2[-1][1]<list1[0][0]: return 0
        if es_init.checkPythonVersion("2.4"):
	   return set(list1)-set(list2)
	else:
	   for idx in xrange(0,len(list1)):
	       if list1[idx]!=list2[idx]:
	          return list1[idx]
	   
	   
    def moveGrade(self,iGrade,oGrade,iTime,oTime):
        """Main method to move a grade in EventStore. It accepts old/new grade and
	timeStamp. Based on user settings it can make a duplicates, exclude some
	runs while moving one grade into another."""
        # check that user provides either svName or runRange
	if not self.svName:
	   # no given data version
	   print "Please provide dataVersionName or run range."
	   return 0
#        if self.svName and (self.minR or self.maxR):
#           print "You need to provide either dataVersionName or run range"
#           return 0
	self.checkRunsInDB(oGrade,oTime)
	
	# form a query for output (readable) grade to check if oGrade/oTime entry exists
        # returns new oTime (if provided oTime="", it will return largest found in DB or
	# day ahead)
	query,oTime = self.formQuery(oGrade,oTime)
	self.updateDBAndLog(query)
	# form a query for input (writable) grade
	query,iTime = self.formQuery(iGrade,iTime)
	self.updateDBAndLog(query)
	tup   = self.fetchAll(query)
	for elem in tup:
	    id = elem[0]
	    adminV = elem[1]
	    timeS  = elem[2]
	    minRun = elem[3]
	    maxRun = elem[4]
	    svid   = elem[5]
	    state  = elem[6]
	    if self.svName and not self.minR and not self.maxR:
	       self.minR = minRun
	       self.maxR = maxRun

	    if state!='active':
	       print "Cannot modify content of Version, state=",state
	       print "grade %s, timeStamp %s, minRun %s, maxRun %s"%(adminV,timeS,minRun,maxRun)
	       sys.exit()

	    # if duplicate request received
	    if not self.badRunList and not self.goodRunList:
	       if minRun>=self.minR and maxRun<=self.maxR:
		  self.startTxn()
		  # check if such entry doesn't exists
		  query="""SELECT id FROM Version WHERE grade='%s' AND timeStamp='%s'
		  AND minRunNumber='%s' AND maxRunNumber='%s' AND graphid='%s'
		  """%(oGrade,oTime,minRun,maxRun,svid)
		  self.updateDBAndLog(query)
		  tup = self.fetchOne(query)
		  # if not, try to insert new range
		  if tup:
		     if self.verbose:
			print "For query",query
			print "we already found an entry in DB"
		  else:
		     query="""INSERT INTO Version 
		     (grade,timeStamp,minRunNumber,maxRunNumber,graphid,state)
		     VALUES ('%s','%s','%s','%s','%s','active')
		     """%(oGrade,oTime,minRun,maxRun,svid)
		     if self.verbose:
			print query
		  self.updateDBAndLog(query)
		  self.endTxn()
		  # clean up minR/maxR for next iteration
		  self.minR=0
		  self.maxR=0
	       else:
		  print "For given runRange %s-%s no match found in ESDB"%(self.minR,self.maxR)
	    else:
	       # form a runList within run range for given grade/timeStamp
	       newRunRanges = self.formNewRunList(iGrade,iTime) 
	       self.startTxn() 
	       
	       for pair in newRunRanges:
		   minR = pair[0]
		   maxR = pair[1]
		   # check if such entry doesn't exists
		   query="""SELECT id FROM Version WHERE grade='%s' AND timeStamp='%s'
		   AND minRunNumber='%s' AND maxRunNumber='%s' AND graphid='%s'
		   """%(oGrade,oTime,minR,maxR,svid)
		   if self.verbose:
		      print query
		   self.updateDBAndLog(query)
		   tup = self.fetchOne(query)
		   # if not, try to insert new range
		   if tup:
		      if self.verbose:
			 print "For query",query
			 print "we already found an entry in DB"
		   else:
		      query="""INSERT INTO Version 
		      (grade,timeStamp,minRunNumber,maxRunNumber,graphid,state)
		      VALUES ('%s','%s','%s','%s','%s','active')
		      """%(oGrade,oTime,minR,maxR,svid)
		      if self.verbose:
			 print query
		   self.updateDBAndLog(query)

	       self.endTxn()
	return 1
#
# main
#
if __name__ == "__main__":

	# initialize user options
	localOpt=["[ -grade <old> <new> ]  [ -time <old> <new> ]"]
	localOpt.append("[ -badRunList <fileName> | -goodRunList <fileName> ]")
	localOpt.append("[ -dataVersionName <name> ] [ -runRange <minR> <maxR> ]")
	localOpt.append("")
	
	usage=es_init.helpMsg("ESVersionManager",localOpt)
	usageDescription="""
Option description (required options marked with '*'):
*       -grade:     specifies the grade, e.g. "physics", "mc-unchecked"
                    you need to provide two of them, 'old' from which you're reading
		    and 'new' to which you're moving
        -time:      specifies the timeStamp, e.g. 20090227
                    you may provide one or two of them, 'old' from which you're reading
		    and 'new' to which you're moving
	-badRunList or -goodRunList
	            specifies the file which contains a list of runs which need to be
	            excluded (badRunList) or out of which new run ranges 
	            need to be generated (goodRunList)
*	-dataVersionName 
	            specifies a data version name associated with given grade.
	            It can be found by using ESDump command.
	-runRange   specifies a run range within ESVersionManager will operate
	"""
	examples="""
To add a new version with same run range:
ESVersionManager -grade physics-unchecked physics -time 0 0 -dataVersionName myVer

To add a new version excluding some runs:
ESVersionManager -grade physics-unchecked physics -badRunList runlist.txt -dataVersionName myVer
runlist.txt contains runs which need to be excluded. Here we didn't specify
timeStamps, which means that ESVersionManager will get this info from DB and append
our information to latest timeStamp for physics grade.

To add a new version with good run list:
ESVersionManager -grade daq-unchecked daq -goodRunList runlist.txt -dataVersionName daq
runlist.txt contains list of good runs
	"""
	userCommand="modVersion.py"
	
	# get EventStore option list
	optList,dictOpt = es_init.ESOptions(userCommand,sys.argv,usage,usageDescription,examples)
	dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
	historyFile,logFile,verbose,profile             = optList[1]
	userCommand                                     = optList[2]+" "
	db, dbType           = es_init.ESDBConnector(dbHost,dbName,userName,userPass,'',dbPort,dbSocket)
	outputLog, globalLog = es_init.ESOutputLog(logFile) 

	iGrade =""
	oGrade =""
	iTime  =""
	oTime  =""
	minR   = 0
	maxR   = 0
	svName = ""
	badRunList = 0
	goodRunList= 0
	x          = 1
	fileRunList= ""
	while x < len(sys.argv):
	    try:
		if sys.argv[x]=="-runRange":
		   minR = int(sys.argv[x+1])
		   maxR = int(sys.argv[x+2])
		   checkArg([minR,maxR])
	           x+=3
		   continue
		if sys.argv[x]=="-dataVersionName":
		   svName = sys.argv[x+1]
		   checkArg([svName])
		   x+=2
		   continue
		if sys.argv[x]=="-grade":
		   iGrade = sys.argv[x+1]
		   oGrade = sys.argv[x+2]
		   checkArg([iGrade,oGrade])
		   x+=3
		   continue
		if sys.argv[x]=="-timeStamp" or sys.argv[x]=="-time":
		   iTime = sys.argv[x+1]
		   oTime = sys.argv[x+2]
		   checkArg([iTime,oTime])
		   x+=3
		   continue
		if sys.argv[x]=="-badRunList" or sys.argv[x]=="-goodRunList":
		   if sys.argv[x]=="-badRunList":
		      badRunList=1
		   else:
		      goodRunList=1
		   fileRunList   = sys.argv[x+1]
		   checkArg([fileRunList])
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

	if not oGrade:
	   print "You need to provide a new grade"
	   print usage
	   sys.exit(1)
	 
	if not svName:
	   print "You need to provide dataVersionName"
	   print usage
	   sys.exit(1)
		
	# form a list of runs which need to be excluded
	runList  = []
	if fileRunList:
	   readList = open(fileRunList,'r').readlines()
	   for idx in xrange(0,len(readList)):
	       runS = string.split(readList[idx])
	       if not len(runS):
		  continue
	       runN = runS[0]
	       if re.search('^[1-9]',runN):
		  runList.append(long(runN))
	   runList.sort()


	pid       = "%s"%os.getpid()
	localtime = "%s"%time.strftime("%H:%M:%S",time.localtime())
	outputLog.write("\n%s %s ###### %s initialization is completed"%(pid,localtime,dbType))

	# create instance of ESManager class
	mydb = ESVersionManager(db,dbType,outputLog)
	mydb.setMinRun(minR)
	mydb.setMaxRun(maxR)
	mydb.setSVName(svName)
	if  badRunList: mydb.setExcludeRunList(runList)
	if goodRunList: mydb.setGoodRunList(runList)
	mydb.setVerboseLevel(verbose)

	# update DB using transaction
	if profile:
	   print "Run in profiler mode"
	   if majorNumber>=2 and minorNumber>=3:
	      profiler = hotshot.Profile("profile.dat")
	      profiler.run("status=mydb.moveGrade(iGrade,oGrade,iTime,oTime)")
	      profiler.close()
	      stats = hotshot.stats.load("profile.dat")
	      stats.sort_stats('time', 'calls')
	      stats.print_stats()
	else:
	   status = mydb.moveGrade(iGrade,oGrade,iTime,oTime)

	rStatus = es_init.ESOutput(status,userCommand,historyFile,outputLog,globalLog)
	sys.exit(rStatus)

	 

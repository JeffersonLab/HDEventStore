#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""ESMoveManager is in charge of moving data in EventStore."""

import os, sys, string, time, glob, stat, shutil
import es_init, file_util, sql_util, gen_util
from es_init import ESInit, checkArg

# include python profiler only if python version greater then 2.3
if es_init.checkPythonVersion("2.3"):
   import hotshot			# Python profiler
   import hotshot.stats			# profiler statistics

class ESMoveManager(ESInit):
    """ESMoveManager is in charge of moving files in EventStore"""
    def __init__(self,db, dbType, logFile):
        ESInit.__init__(self, db, dbType, logFile)

    def moveFilesInES(self,iFileList,fileOut):
        """High-level method to move a list of files to fileOut destination.
        All job is done by using L{moveFileInES} method.
        @type iFileList: list
        @param iFileList: list of files
        @type fileOut: string
        @param fileOut: file name
        @rtype: integer
        @return: status code
        """
        if len(iFileList)>1 and not os.path.isdir(fileOut):
	   print "Output directory %s doesn't exists"%fileOut
	   return self.error
	fstat = os.stat(fileOut)
	perm  = oct(stat.S_IMODE(fstat[stat.ST_MODE]))
	if int(perm[1]) < 6: # on Unix 6 is enough to write
	   print "ERROR: you don't have permission to write to",fileOut
	   return self.error	   
	   
	fileInList = []
	for iFile in iFileList:
	    print "Processing",iFile
	    fileType = file_util.fileType(iFile)
	    if self.allow(fileType):
	       fileInList.append(iFile)
	self.writeToLog("moveFilesInES")
	if self.verbose: print "Using moveFilesInES"
	status = self.error
	okList = []
	compList = []
	for file in fileInList:
	    if os.path.isfile(file):
	       status, query, cQuery = self.moveFileInES(file,fileOut)
	       if status == self.error:
		  print "############"
		  print "Fail to move %s %s"%(file,fileOut)
		  print "##############################################"
		  print "List of successfull queries:"
		  for item in okList: print item
		  print "##############################################"
		  print "List of compensating queries:"
		  for item in compList: print item
		  print "##############################################"
		  print "MAKE YOUR DECISION. Either invoke compensating"
		  print "queries or proceed."
		  break
	       okList.append(query)
	       compList.append(cQuery)
	return status
	
    def moveFileInES(self,fileIn,fileOut):
        """Move a single fileIn to fileOut destination. First update FileID table and 
	then physically copy file to new location.
        @type fileIn: string
        @param fileIn: file name
        @type fileOut: string
        @param fileOut: file name
        @rtype: tuple
        @return: (status code, query, compensatingQuery), where query is SQL query how
        to update FileID table. It's printed out by L{moveFilesInES} in the case of
        failure for debugging purpose.
        """
	query = ""
	cQuery= ""
	# I need to add log what I'm doing since updating DB and
	# moving/copying file are independent operations
	if self.verbose: 
	   print "Using moveFileInES"
	   print "Attempt to move %s to %s"%(fileIn,fileOut)
	if os.path.isfile(fileIn):
	   query = "SELECT fileId FROM FileID WHERE fileName='%s'"%fileIn
	   tup = self.fetchOne(query)
	   if tup and len(tup):
	      id = tup[0]
	      moveTo = fileOut
	      if os.path.isdir(fileOut):
		 moveTo = os.path.join(fileOut,os.path.split(fileIn)[1])
	      # first copy file to new location
	      try:
		 shutil.copy2(fileIn,fileOut)
	      except:
		 gen_util.printExcept()
		 print "Cannot copy %s to %s"%(fileIn,fileOut)
		 return (self.error, "N/A", "N/A")
	      query = "UPDATE FileID SET fileName='%s' WHERE fileID='%s'"%(moveTo,id)
	      cQuery= "UPDATE FileID SET fileName='%s' WHERE fileID='%s'"%(fileIn,id)
	      try:
		 self.startTxn()
		 self.updateDBAndLog(query)
		 self.endTxn()
		 if self.verbose:
		    print query
	      except:
		 gen_util.printExcept()
		 print "Fail to update DB with the following query"
		 print query
		 print "------------------------------------------"
		 print "You MUST invoke the following command to compensate"
		 print cQuery
		 return (self.error, query, cQuery)
	      try:
		 os.remove(fileIn)
		 if self.verbose:
		    print "File %s has been successfully moved"%fileIn
	      except:
		 gen_util.printExcept()
		 print "WARNING: cannot remove %s"%fileIn
		 return (self.ok, query, cQuery)
	      return (self.ok, query, cQuery)
	return (self.error, query, cQuery) # error
#
# main
#
if __name__ == "__main__":
	localOpt=["[ -move <fileIn> <fileOutDir> ]"]
	usage=es_init.helpMsg("ESMove",localOpt)
	usageDescription="""
Option description:
	"""

	examples   = es_init.ESExamples()
	userCommand="ESMove.py"
	optList, dictOpt = es_init.ESOptions(userCommand,sys.argv,usage,usageDescription)
	dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
	historyFile,logFile,verbose,profile             = optList[1]
	userCommand                                     = optList[2]
	
	# default values
	fileInList=[]
	fileIn  = ""
	fileOut = ""
	# parse the rest of the options and form user's command
	x = 1
	doNotRead= 0
	counter = 0
	while x < len(sys.argv):
          try:
	     if sys.argv[x] == "-move":
	        # first check if pattern is present
	        fileList=[]
		for idx in xrange(x+1,len(sys.argv)):
		    newArg  = sys.argv[idx]
		    counter+=1
		    if newArg[0]=="-":
		       break
		    fileList.append(newArg)
                # do some checking
                fileOut = fileList[-1]
                if os.path.isdir(fileList[0]) and not os.path.isdir(fileList[-1]):
		   print "Final destination should be a directory name"
		   raise
		for idx in xrange(0,len(fileList)-1):
		    if os.path.isdir(fileList[idx]):
		       for file in os.listdir(fileList[idx]):
			   fileInList.append(os.path.join(fileList[idx],file))
                    if os.path.isfile(fileList[idx]):
		       fileInList.append(fileList[idx])
		checkArg(fileInList)
   	        checkArg([fileOut])
		x+=counter
		continue
	     # if we reach here, that means we found unkown option
	     if dictOpt.has_key(sys.argv[x]):
		x+=dictOpt[sys.argv[x]]
	     else:
		print "Option '%s' is not allowed"%sys.argv[x]
		raise
          except:
             sys.exit(1)
	
	# connect to MySQL EventStoreDB
	outputLog, globalLog = es_init.ESOutputLog(logFile) 
	db, dbType           = es_init.ESDBConnector(dbHost,dbName,userName,userPass,'',dbPort,dbSocket)
	pid                  = "%s"%os.getpid()
	localtime            = "%s"%time.strftime("%H:%M:%S",time.localtime())
	outputLog.write("\n%s %s ###### %s initialization is completed"%(pid,localtime,dbType))

	# create instance of ESManager class
	mydb = ESMoveManager(db,dbType,outputLog)

	# update DB using transaction
	if profile:
	   print "Run in profiler mode"
	   if es_init.checkPythonVersion("2.3"):
	      profiler = hotshot.Profile("profile.dat")
	      profiler.run("status=mydb.moveFilesInES(fileInList,fileOut)")
	      profiler.close()
	      stats = hotshot.stats.load("profile.dat")
	      stats.sort_stats('time', 'calls')
	      stats.print_stats()
	else:
	   status = mydb.moveFilesInES(fileInList,fileOut)

	rStatus = es_init.ESOutput(status,userCommand,historyFile,outputLog,globalLog)
	sys.exit(rStatus)


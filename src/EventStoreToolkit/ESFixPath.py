#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2005
#
"""Collection of tools to fix arbitrary file paths wrt EventStore specifications.
Please consult https://wiki.lepp.cornell.edu/CleoSWIG/bin/view/Main/EventStoreDesign
web page for further reading."""

import os, sys, string, socket, shutil
import sql_util, esdb_auth, es_init, ESManager, file_util, os_path_util, gen_util

# release path
RELPATH = '/nfs/solaris2/cleo3/Offline/rel'  ### CHECK

def newParentList(sql,run,parentList):
    """Check every parent if it holds given run"""
    # we need to make one more lookup in DB and search if given parent
    # cover this run
    newParentList=[]
    for item in parentList:
        query = """SELECT DISTINCT minRunNumber,maxRunNumber FROM 
	Version,GraphPath,SpecificVersion 
	WHERE Version.graphid=GraphPath.graphid 
	AND GraphPath.svid=SpecificVersion.svid 
	AND SpecificVersion.svName='%s'
	"""%item
	tup   = sql.fetchAll(query) 
	for elem in tup:
	    minR = int(elem[0])
	    maxR = int(elem[1])
	    if minR<=int(run) and maxR>=int(run):
	       if not newParentList.count(item):
                  newParentList.append(item)
    return newParentList

### CHECK
def formNewPath(prefix,run,release,svName,parentList,eventType="event"):
    """Form a new path according to CLEOc data path specifications:
    /cleo/{detector,simulated}/{event,calibration}/{daq,pass2_version}/123400/123456/{specific_version_path}  
    """
    
    dataType = "detector"
    if string.lower(svName)[:2]=='mc':
       dataType = "simulated"
    # form runHundren basis
    runS = "%s"%run
    runH = runS[:4]+"00"
    # form new path
    newPath = prefix+"/cleo/"+dataType+"/"+eventType+"/"+release+"/"+runH+"/"+runS+"/"
    # parent list should be in reverse order
    parentList.reverse()
    for item in parentList:
        if string.lower(item)=='daq':
	   continue
	if string.lower(item)!='null':
	   newPath+=item+"/"
    return os.path.normpath(newPath)
	
def getRelease(relList,svName):
    """Lookup in release list and find out a match between release name and svName.
    Return empty string if no match found."""
    for rel in relList:
        if svName.find(rel)!=-1:
	   return rel
    return ""

### CHECK    
def ESFixPath(args):
    """Fix paths in EventStoreDB. The CLEOc data path specifications:
    /cleo/{detector,simulated}/{event,calibration}/{daq,pass2_version}/123400/123456/{specific_version_path}
    """
    localOpt = ["[ -prefix <Site CLEOc data prefix, e.g. /cdat> ]"]
    usage    = es_init.helpMsg("ESFixPath",localOpt)
    usageDescription=""
	    
    examples="""
    """

    userCommand="ESFixPath.py"
    optList, dictOpt = es_init.ESOptions(userCommand,args,usage,usageDescription,examples)
    dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
    historyFile,logFile,verbose,profile             = optList[1]
    userCommand                                     = optList[2]
    
    prefix = "/cdat/"   ### CHECK
    x         = 1
    while x < len(args):
	try:
	    if string.lower(args[x]) == "-prefix":
	       prefix = args[x+1]
	       x+=2
	       continue
	    if dictOpt.has_key(args[x]):
	       x+=dictOpt[args[x]]
	    else:
	       print "Option '%s' is not allowed"%args[x]
	       raise
	except:
	    sys.exit(1)

    if prefix[0]!="/":
       print "Prefix should start with /"
       print usage
       sys.exit(1)

    # initialize log
    outputLog, globalLog = es_init.ESOutputLog(logFile)
	
    # connect to dbIn EventStore
    db, dbType = es_init.ESDBConnector(dbHost,dbName,userName,userPass)
    sql        = sql_util.SQLUtil(db,dbType,outputLog)
    tableList  = sql.getTables()
    
    # Initialize ESManager for dbOut
    es_init.ESInput(userCommand,outputLog,dbType)

    # lock all tables for write operation in dbOut
    sql.lockTables()
    
    # postpone all commits to DB, once job will finish we'll invoke commit.
    sql.setCommitFlag(0)
	
    sql.startTxn()
    try:
        # read all releases (from Solaris) which would be used to find out pass2_version
        relList = os.listdir(RELPATH)
	
	# start processing
	print "Processing:"
	sys.__stdout__.flush()
	# we can replace retrieval of all files from DB, by using min/max fileId and then
	# loop over file Ids.
#        query="SELECT fileId,fileName FROM FileID"
#        tup  = sql.fetchAll(query)
#        for item in tup:
#            fileId = item[0]
#            file   = item[1]
        query="SELECT MIN(fileId),MAX(fileId) FROM FileID"
	tup  = sql.fetchOne(query)
	minId= long(tup[0])
	maxId= long(tup[1])+1
	for fileId in xrange(minId,maxId):
	    query  = "SELECT fileName FROM FileID WHERE fileId='%s'"%fileId
	    tup    = sql.fetchOne(query)
	    if not tup: continue
	    file   = tup[0]
	    if not os_path_util.isFile(file):
	       print "Found non existing file",file
	       continue
	    # Open KeyFile table, locate fileId, graphid=>svName (with all parents)
	    # create a new link
	    keyFile= file
	    query  ="SELECT run,graphid FROM KeyFile WHERE keyFileId='%s'"%fileId
	    tup    = sql.fetchAll(query)
	    for item in tup:
		run  = item[0]
		gid  = item[1]
		query="""SELECT svName FROM SpecificVersion,GraphPath 
		WHERE GraphPath.svid=SpecificVersion.svid 
		AND GraphPath.graphid='%s'"""%gid
		tup = sql.fetchOne(query) # FIXME, there're many svid's assigned to gid
		svName  = tup[0]
		dir,fileName = os.path.split(keyFile)
		dList,idList,dict,dictId,graph=sql.getAllParents(svName)
	        parentList = newParentList(sql,run,dList)
	        if string.lower(svName)=='daq':
		   release = 'daq'
		else:
                   release = getRelease(relList,svName)
		newPath = formNewPath(prefix,run,release,svName,parentList)
		newDir  = os.path.join(newPath,'index')
		if not os.path.isdir(newDir):
		   os.makedirs(newDir)
		newFile = os.path.join(newDir,fileName)
		print "Link (key)",newFile,"=>",keyFile
		sys.__stdout__.flush()
		if not os_path_util.isFile(newFile):
		   os.symlink(file,newFile)
	        # change db entry
	        query="UPDATE FileID SET fileName='%s' WHERE fileId='%s'"%(newFile,fileId)
	        sql.updateDBAndLog(query)
	    # Open Location table, locate fileId, graphid=>svName and open
	    # loc. file header to locate data files
	    locFile  = file
	    query    = "SELECT run,graphid FROM Location WHERE locationFileId='%s'"%fileId
	    tup      = sql.fetchAll(query)
	    for item in tup:
		run  = item[0]
		gid  = item[1]
		query="""SELECT svName FROM SpecificVersion,GraphPath 
		WHERE GraphPath.svid=SpecificVersion.svid 
		AND GraphPath.graphid='%s'"""%gid
		tup = sql.fetchOne(query) # FIXME, there're many svid's assigned to gid
		svName  = tup[0]
		dir,fileName = os.path.split(locFile)
		dList,idList,dict,dictId,graph=sql.getAllParents(svName)
	        parentList = newParentList(sql,run,dList)
	        if string.lower(svName)=='daq':
		   release = 'daq'
		else:
   	           release = getRelease(relList,svName)
		newPath = formNewPath(prefix,run,release,svName,parentList)
		newDir  = os.path.join(newPath,'index')
		if not os.path.isdir(newDir):
		   os.makedirs(newDir)
		newFile = os.path.join(newDir,fileName)
		print "Link (loc)",newFile,"=>",locFile
		sys.__stdout__.flush()
		if not os_path_util.isFile(newFile):
		   os.symlink(locFile,newFile)
	        # change db entry
	        query="UPDATE FileID SET fileName='%s' WHERE fileId='%s'"%(newFile,fileId)
	        sql.updateDBAndLog(query)
	
		# open loc. file header and get data file id's
		query="SELECT fileName,fileId FROM FileID WHERE"
		count=0
		for id in file_util.getFileIds(locFile):
		    if not count:
		       query+= " fileId='%s'"%id
		       count =1
		    else:
		       query+= " OR fileId='%s'"%id
		tup = sql.fetchAll(query)
		for item in tup:
		    datFile = '%s'%item[0]
	            if not os_path_util.isFile(datFile):
	               print "Found non existing file",datFile
	               continue
		    fileId  = item[1]
		    dir,fileName = os.path.split(datFile)
		    newDir  = os.path.join(newPath,'data')
		    if not os.path.isdir(newDir):
		       os.makedirs(newDir)
		    newFile = os.path.join(newDir,fileName)
		    print "Link (dat)",newFile,"=>",datFile
		    sys.__stdout__.flush()
		    if not os_path_util.isFile(newFile):
		       os.symlink(datFile,newFile)
	            # change db entry
	            query="UPDATE FileID SET fileName='%s' WHERE fileId='%s'"%(newFile,fileId)
	            sql.updateDBAndLog(query)
    except:
        print "Caught an error during merging step."
        gen_util.printExcept()
	db.rollback()
	return

    # everything is ready for commit
    sql.setCommitFlag(1)
    sql.endTxn()
    sql.commit()
    sql.unlockTables()
    sql.close()
    returnStatus = es_init.ESOutput(1,userCommand,historyFile,outputLog,globalLog)
    return returnStatus

#
# main
#
if __name__ == "__main__":
    ESFixPath(sys.argv)

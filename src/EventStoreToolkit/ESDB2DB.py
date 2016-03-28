#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2005
#
"""A set of tools to merge one EventStore DB to another"""

import os, sys, string, socket, shutil, re
import sql_util, esdb_auth, es_init, ESManager, file_util, os_path_util, gen_util

def db2db(args):
    """A tool to merge one EventStore DB into another. We use the following algorithm:
         - lock tables of output DB for writing
         - loop over available grade,timeStamps in input DB => get graphid
         - update Version table in output DB
         - loop over files in FileID
         - using new graphid and new (fileName,fileType) update KeyFile 
           and Location tables. In the case of location file, change fileIds in its
           header to new values. In both cases change fileName of index/location files.
         - update FileID with new fileName (if any, in the case of data file we don't
           change its name).
         - rename the file.
    Please note, we only intend to change a prefix of files and rely on the fact
    that all files in input DB were properly configured (according with EventStore
    specs. This routine accepts a list of command line arguments to do the merging.
    Here an example:

    ESDB2DB -dbin EventStore1@/home/vk/sqlite.db -dbout EventStore2@lnx151 -changeprefix /nfs /cdat
    """
    msg="""
Usage: ESDB2DB [ -help ] [ --help ] [ -examples ] [ -profile ]
               [ -verbose ] [ -historyfile <filename> ] 
               [ -logFile </path/filename or 'stdout' or 'stderr'> ]
	       [ -dbin  [<dbName>@<dbHost> or <fileName>] ] 
	       [ -dbout [<dbName>@<dbHost> or <fileName>] ]
	       [ -changeprefix <fromDir> <toDir> ]
    """
    msg+= "\n"
    msg+= "\nOptions can be specified in any order."
    msg+= "\nFor option description please run 'ESDB2DB --help'"
    msg+= "\nFor use cases please run 'ESDB2DB -examples'"
    msg+= "\nContact: Sean Dobbs, s-dobbs@northwestern.edu\n"
    usage = msg
    usageDescription=""
	    
    examples="""
    Examples:	  
    to merge content of SQLite EventStore1 DB into MySQL EventStore2 DB
	   ESDB2DB -dbin EventStore1@/home/vk/sqlite.db -dbout EventStore2@lnx151
    to merge content of SQLite EventStore1 DB into MySQL EventStore2 DB, plus
    change prefix of all files in SQLite from /nfs to /cdat
	   ESDB2DB -dbin EventStore1@/home/vk/sqlite.db -dbout EventStore2@lnx151
	           -changeprefix /nfs /cdat
    """

    userCommand="ESDB2DB.py"
    optList, dictOpt = es_init.ESOptions(userCommand,args,usage,usageDescription,examples)
    dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
    historyFile,logFile,verbose,profile             = optList[1]
    userCommand                                     = optList[2]

    dbInHost=bInName=dbOutHost=dbOutHost=oldDir=newDir=""
    x         = 1
    inOut     = 0
    while x < len(args):
	try:
	    if string.lower(args[x]) == "-dbin" : 
               dbInName,dbInHost,dbInPort,dbInSocket=es_init.decodeHostNameString(args[x+1])
	       inOut+=1
	       x+=2
	       continue
	    if string.lower(args[x]) == "-dbout" : 
               dbOutName,dbOutHost,dbOutPort,dbOutSocket=es_init.decodeHostNameString(args[x+1])
	       inOut+=1
	       x+=2
	       continue
	    if string.lower(args[x]) == "-changeprefix":
	       oldDir = args[x+1]
	       newDir = args[x+2]
	       x+=3
	       continue
	    if dictOpt.has_key(args[x]):
	       x+=dictOpt[args[x]]
	    else:
	       print "Option '%s' is not allowed"%args[x]
	       raise
	except:
            gen_util.printExcept()
	    sys.exit(1)

    if inOut!=2:
       print usage
       sys.exit(1)

    if newDir:
       if not os_path_util.checkPermission(newDir):
          print "You don't have permission to write to",newDir
	  sys.exit(1)
	
    # initialize log
    outputLog, globalLog = es_init.ESOutputLog(logFile)
	
    # connect to dbIn EventStore
    dbIn, dbInType = es_init.ESDBConnector(dbInHost,dbInName,userName,userPass,'EXCLUSIVE',dbInPort,dbInSocket)
    sqlIn          = sql_util.SQLUtil(dbIn,dbInType,outputLog)
    inTableList    = sqlIn.getTables()
    
    # in the case of SQLite we need to open DB with isolation level EXCLUSIVE
    # in order to lock all tables.
#    dbOut,dbOutType= es_init.ESDBConnector(dbOutHost,dbOutName,userName,userPass,'EXCLUSIVE',dbPort,dbSocket)
    dbOut,dbOutType= es_init.ESDBConnector(dbOutHost,dbOutName,userName,userPass,'',dbOutPort,dbOutSocket)
    sqlOut         = sql_util.SQLUtil(dbOut,dbOutType,outputLog)
    outTableList   = sqlOut.getTables()
    if verbose:
       print "input  table list:",inTableList
       print "output table list:",outTableList
    if inTableList!=outTableList:
       for table in inTableList:
           if not outTableList.count(table) and table!='MaxMasterID':
	      if verbose:
	         print "No '%s' EventStore table found, we will create it"%table
	      sqlOut.createTables(table)

    # we're going to use a dictionary of old/new ids for data files
    dict = {}
	
    # Initialize ESManager for dbOut
    es_init.ESInput(userCommand,outputLog,dbOutType)
    esManager = ESManager.ESManager(dbOut,dbOutType,outputLog)
    esManager.setVerboseLevel(verbose)

    # lock all tables for write operation in dbOut
    esManager.lockTables()
    
    # postpone all commits to DB, once job will finish we'll invoke commit.
    esManager.setCommitFlag(0)

    # get list of parents from dbIn
    parentList=[]
    query="SELECT parentId FROM PathDepend"
    tup  = sqlIn.fetchAll(query)
    for item in tup:
        parentId = item[0]
        query="""SELECT svName FROM GraphPath,SpecificVersion WHERE 
	         GraphPath.svid=SpecificVersion.svid AND
		 graphid='%s'"""%parentId
	data = sqlIn.fetchAll(query)
	for name in data:
	    if not parentList.count(name[0]):
	       parentList.append(name[0])

    # Clean-up dbIn and remove empty fileNames (they may be created in the case of dskim when
    # parent files where not store into db)
    query = "DELETE FROM FileID where fileName IS NULL"
    tup   = sqlIn.updateDBAndLog(query)
    
    # First step is to modify path prefix in dbIn to new one
    if newDir:
       print "Modifying original DB to use new path prefix"
       try:
	   query = "SELECT fileId,fileName FROM FileID"
	   tup   = sqlIn.fetchAll(query)
	   for item in tup:
	       fileId  = item[0]
	       fileName= item[1]
	       # replace fileName with new fileId
	       newFileName = os.path.normpath(gen_util.changeFileName(fileName,oldDir,newDir))
	       query = "UPDATE FileID SET fileName='%s' WHERE fileId='%s'"%(newFileName,fileId)
	       sqlIn.updateDBAndLog(query)
	       if verbose: print fileName,"=>",newFileName
       except:
	   dbIn.rollback()
	   print "Caught an error during merging step."
	   gen_util.printExcept()
	   status = es_init.ESOutput(0,userCommand,historyFile,outputLog,globalLog)
	   return status
       # commit changes
       dbIn.commit()
	
    # put everything in try/except block, if something bad happens we'll rollback
    maxFileId = 0
    if dbOutType=="sqlite":
       query="BEGIN IMMEDIATE"
       sqlOut.updateDBAndLog(query)
    try:
        # get max fileId from dbOut, in the case if we'll need to rollback
        query = "SELECT MAX(fileId) FROM FileID"
	tup   = sqlOut.fetchOne(query)
	maxFileId = tup[0]
       	
	# get info from Version dbIn.
	query = """SELECT grade,timeStamp,minRunNumber,maxRunNumber,
		   Version.graphid,svName 
		   FROM Version,GraphPath,SpecificVersion 
		   WHERE Version.graphid=GraphPath.graphid 
		   AND GraphPath.svid=SpecificVersion.svid"""
	tup = sqlIn.fetchAll(query)
	dictGraphId= {}
	lastGraphId= 0
        locList    = []
        keyList    = []
	# loop over entries in Version dbIn and fill out Version dbOut.
	for item in tup:
	    _grade = item[0]
	    _timeS = item[1]
	    _minR  = item[2]
	    _maxR  = item[3]
	    _gid   = item[4]
	    _svName= item[5]

	    # set-up all parameters
	    esManager.setSVName(_svName)
	    esManager.setGrade(_grade)
	    esManager.setTimeStamp(_timeS)
	    esManager.setMinRun(_minR)
	    esManager.setMaxRun(_maxR)
	    esManager.setParents(parentList)
		
	    # update GraphPath, SpecificVersion tables
	    newGraphId=esManager.updateVersion()
	    if not newGraphId:
	       print "Fail to update Version table"
	       sys.exit(1)
	    # fill out dictionary of old->new graphid's
	    if not dictGraphId.has_key(_gid):
	       dictGraphId[_gid]=newGraphId

	parentgidList=""
	for parent in parentList:
	    query="""SELECT graphid FROM GraphPath,SpecificVersion WHERE
		     GraphPath.svid=SpecificVersion.svid AND
		     SpecificVersion.svName='%s'"""%parent
	    parentgidList= sqlOut.fetchAll(query)
	# Loop over FileID table in dbIn and update FileID, KeyFile, Location, FileType, 
	# RunUID tables in dbOut
	#
	# while inserting into dbOut we need to change file names
	query="SELECT fileId FROM FileID"
	tup  = sqlIn.fetchAll(query)
	print "Processing:"
	for item in tup:
	    fileId  = item[0]
	    query   = """SELECT fileName,type FROM FileID,FileType 
			 WHERE FileID.typeId=FileType.id AND fileId=%s"""%fileId
	    tup     = sqlIn.fetchOne(query)
#            print query
#            print tup
	    if not tup:
	       continue
	    fileName= tup[0]
	    fileType= tup[1]
            print fileName
	    # check dbOut if file already there (e.g. we used parents)
            query   = "SELECT fileId FROM FileID where fileName='%s'"%fileName
            tup     = sqlOut.fetchOne(query)
            if tup and tup[0]:
               if verbose:
                  print "Found '%s' in dbOut with fileId=%s"%(fileName,tup[0])
               dict[fileId]=tup[0]
	       # look-up Location table and save it to the list to preserve order of loc.files
	       query="SELECT id,run,uid,graphid FROM Location WHERE locationFileId=%s"%fileId
	       tup = sqlIn.fetchAll(query)
	       for item in tup:
		   id  = item[0]
		   run = item[1]
		   uid = item[2]
		   gid = item[3]
                   try:
		      newGraphId = dictGraphId[gid]
                   except:
                      continue
                   locList.append((id,newGraphId,run,uid,fileId))
	       # look-up KeyFile table and save it 
	       query="SELECT graphid,view,run,uid FROM KeyFile WHERE keyFileId=%s"%fileId
	       tup = sqlIn.fetchAll(query)
	       for item in tup:
		   gid = item[0]
		   view= item[1]
		   run = item[2]
		   uid = item[3]
                   try:
		      newGraphId = dictGraphId[gid]
                   except:
                      continue
                   keyList.append((newGraphId,view,run,uid,fileId))
               continue
	    # change file permission to be user-writable
	    if fileType=='levio':
	       os.chmod(fileName,0644)
	    query   = "SELECT id FROM FileType WHERE type='%s'"%fileType
	    try:
	       tup  = sqlOut.fetchOne(query)
	    except:
	       print "No typeId found for %s"%fileType
	       raise
	    if not tup:
	       typeId = esManager.updateFileType(fileName)
	    else:
	       typeId = tup[0]	
	    # get new file id
	    newFileId = esManager.getIds(1)
	    dict[fileId]=newFileId
	    # make new fileName and copy loc. files and make hard link for key files
	    #if fileType=='ikey' or fileType=='levio' or fileType=='lhddm':
	    #   fromField  = 'esdb-%s'%fileId
	    #   toField    = 'esdb-%s'%newFileId
	    #   newFileName=gen_util.changeFileName(fileName,fromField,toField)
	    #   dir,file = os.path.split(newFileName)
	    #   if not os.path.isdir(dir):
	    #      if verbose:
            #   print "Create",dir
	    #      os.makedirs(dir)
	    #   if fileType=='ikey':
	    #      if not os.path.isfile(newFileName):
	    #         os.link(fileName,newFileName)
	    #      else:
	    #         if not os.path.islink(newFileName):
	    #	        print "File '%s' not found"%newFileName
	    #	        raise
	    #      if verbose:
	    #         print "Link",fileName,newFileName
	    #   else:
            #      if fileName!=newFileName:
	    #         shutil.copy(fileName,newFileName)
	    #      if verbose:
	    #         print "Copy",fileName,newFileName
	    #else:
            newFileName=fileName

	    if fileType=='ikey':
	       # update KeyFile table
	       query="SELECT view,run,uid,graphid FROM KeyFile WHERE keyFileId=%s"%fileId
	       tup = sqlIn.fetchAll(query)
	       for item in tup:
		   view= item[0]
		   esManager.setView(view)
		   run = item[1]
		   uid = item[2]
		   gid = item[3]
		   newGraphId = dictGraphId[gid]
		   esManager.updateKeyFile(newGraphId,view,run,uid,newFileId)
		   # we also need to make a copy of key file ids for all parents
#                   for parentgid in parentgidList:
#                       query="""SELECT keyFileId,view FROM KeyFile WHERE 
#                                graphid='%s' AND run='%s' AND uid='%s'"""%(parentgid[0],run,uid)
#                       data = sqlOut.fetchAll(query)
#                       for id in data:
#                           kid  = id[0]
#                           view = id[1]
			   # check if we already injected key file into newGraphId
#                           query="""SELECT keyFileId FROM KeyFIle WHERE graphid='%s'
#                                    AND view='%s' AND run='%s' 
#                                    AND uid='%s'"""%(newGraphId,view,run,uid)
#                           tup  = sqlOut.fetchOne(query)
#                           if not tup:
#                              esManager.updateKeyFile(newGraphId,view,run,uid,kid)
			       
	       # update RunUID table
	       esManager.updateRunUID(run,uid)
	    # update FileID table
            if verbose:
               print "Update:",newFileId,newFileName,typeId
	    esManager.updateFileID(newFileId,newFileName,typeId)

        # update KeyFile table
        for item in keyList:
            gid       = item[0]
            view      = item[1]
            run       = item[2]
            uid       = item[3]
            keyFileId = item[4]
            # check if this entry exists in DB
            query = """SELECT graphid FROM KeyFile WHERE graphid='%s' AND view='%s' AND run='%s'
                       AND uid='%s' AND keyFileId='%s'"""%(gid,view,run,uid,keyFileId)
            tup   = sqlOut.fetchOne(query)
            if tup:
               if verbose:
                  msg = "Found (%s,%s,%s,%s,%s)"%(gid,view,run,uid,keyFileId)
                  msg+= "in KeyFile table, no update necessary"
            else:
               esManager.updateKeyFile(gid,view,run,uid,keyFileId)
    except:
        print "Caught an error during merging step."
        gen_util.printExcept()
        # something wrong, loop over new entries in dbOut and remove newly created files
	query = """SELECT fileName FROM FileID WHERE fileId='%s'"""%maxFileId
	tup   = sqlIn.fetchAll(query)
	origList = []
	for item in tup:
	    origList.append(item[0])
	
        query = """SELECT fileName,type FROM FileID,FileType WHERE 
	           FileID.typeId=FileType.id AND fileId>'%s'"""%maxFileId
        tup   = sqlOut.fetchAll(query)
        for item in tup:
	    fileName = item[0]
	    fileType = item[1]
	    if fileType=='ikey' or fileType=='lpds' or fileType=='lbin' or fileType=='lhddm':
	       os.chmod(fileName,0644)
	       if verbose: print "Remove",fileName
	       try:
	          if not origList.count(fileName):
		     os.remove(fileName)
	       except:
		  pass
	# remove as well last file we process
#        if oldDir!=newDir:
#           if verbose: print "Remove",newFileName
#           try:
#              os.chmod(newFileName,0644)
#              os.remove(newFileName)
#           except:
#              pass

        # let's rollback
	if verbose: print "Invoke rollback"
        dbOut.rollback()
        esManager.rollback()
        status = es_init.ESOutput(0,userCommand,historyFile,outputLog,globalLog)
        return status

    #if dbOutType=="sqlite":
    #   query="COMMIT"
    #   sqlOut.updateDBAndLog(query)
    query = """SELECT fileName,type FROM FileID,FileType WHERE 
	       FileID.typeId=FileType.id"""
    tupOut= sqlOut.fetchAll(query)
    dbOut.commit()
    esManager.unlockTables()
    esManager.close()

    # everything is fine and we may clean-up original files
    query = """SELECT fileName,type FROM FileID,FileType WHERE 
	       FileID.typeId=FileType.id"""
    tupIn = sqlIn.fetchAll(query)
    listOut = list(tupOut)
    for item in tupIn:
        if listOut.count(item): continue
	fileName = item[0]
	fileType = item[1]
	if fileType=='ikey' or fileType=='levio' or fileType=='lhddm':
	   if verbose: print "Remove",fileName
	   try:
	      os.remove(fileName)
	   except:
	      pass
    returnStatus = es_init.ESOutput(1,userCommand,historyFile,outputLog,globalLog)
    return returnStatus

#
# main
#
if __name__ == "__main__":
    db2db(sys.argv)

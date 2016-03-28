#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2005
#
"""The ESGetComment function allows get comments from EventStore."""

import os, sys, string, re, time
import es_init,esdb_auth, os_path_util
from es_init import ESInit, checkArg

def ESGetComment(args):
    """ESGetComment was designed to get comments from EventStore DB.
    In order to use it youm need to provide grade/timeStamp
    and a date (or date range) of desired comments.

    Please note, ESGetComment is a wrapper shell script around getComment.py
    module which does the work.
    """
    localOpt=["[ -grade <grade> -timeStamp <time> ]"]
    localOpt.append("[ -date <t1 t2> ] [ -dataVersionName <name> ]")
    
    usage=es_init.helpMsg("ESGetComment",localOpt)
    usageDescription="""Option description:
    Retrieve comments from EventStore for given grade/timeStamp or dataVersionName
    and date.
    """
    examples="""
    Examples:
    # get all comments happened on 20090101
    ESGetComment -date 20090101

    # get all comments between 20090101 20090202
    ESGetComment -date 20090101 20090202

    # get all comments for PP2-20090101 between 20090101 20090202
    ESGetComment -dataVersionName PP2-20090101 -date 20090101 20090202

    # get all comments for physics/20050323 between 20090101 20090202
    ESGetComment -grade physics -timeStamp -date 20090101 20090202
    """

    userCommand="getComment.py"
    optList, dictOpt = es_init.ESOptions(userCommand,args,usage,usageDescription,examples)
    dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
    historyFile,logFile,verbose,profile             = optList[1]
    userCommand                                     = optList[2]
    
    grade     =""
    timeS     =""
    time1     = time.strftime("%Y%m%d",time.localtime())
    time2     = time1
    x         = 1
    svName    = ""
    comment   = "%s -- "%time.strftime("%H:%M:%S",time.localtime())
    duplicateDBEntry=0

    while x < len(args):
	try:
	    if args[x]=="-dataVersionName":
	       svName = args[x+1]
	       checkArg([svName])
	       x+=2
	       continue
	    if args[x]=="-grade":
	       grade = args[x+1]
	       checkArgument([grade])
	       x+=2
	       continue
	    if args[x]=="-timeStamp":
	       timeS = args[x+1]
	       checkArg([timeS])
	       x+=2
	       continue
	    if args[x]=="-date":
	       time1 = args[x+1]
	       if x+2<len(args):
		  time2 = args[x+2]
		  if time2[0]=="-":
		     time2=time1
		     x+=2
		     continue
		  x+=1
	       x+=2
	       continue
	    # if we reach here, that means we found unkown option
	    if dictOpt.has_key(args[x]):
	       x+=dictOpt[args[x]]
	    else:
	       print "Option '%s' is not allowed"%args[x]
	       raise
	except:
	    sys.exit(1)

    if grade and svName:
       print "Both grade=%s and dataVersionName=%s found"%(grade,svName)
       print usage
       sys.exit(1)
     
    if (grade and not timeS) or (not grade and timeS):
       print "You need to specify both grade and timeStamp"
       print usage
       sys.exit(1)
     
    if len(time1)!=8 or len(time2)!=8:
       print "Incorrect time format found, please use YYYYMMDD format"
       sys.exit(1)
	    
    # connect to MySQL EventStoreDB
    outputLog, globalLog = es_init.ESOutputLog(logFile) 
    db, dbType           = es_init.ESDBConnector(dbHost,dbName,userName,userPass,'',dbPort,dbSocket)
    pid                  = "%s"%os.getpid()
    localtime            = "%s"%time.strftime("%H:%M:%S",time.localtime())
    outputLog.write("\n%s %s ###### %s initialization is completed"%(pid,localtime,dbType))
    if dbType=="sqlite":
       addToQuery=""
    else:
       addToQuery=" FOR UPDATE"
    cu = db.cursor()

    if not svName and not grade:
       query = """SELECT CommentDate,Comment FROM SpecificVersionComment WHERE
		  CommentDate>='%s' AND CommentDate<='%s'"""%(time1,time2)
    elif len(svName):
       query = """SELECT CommentDate,Comment 
		  FROM SpecificVersion,SpecificVersionComment WHERE
		  CommentDate>='%s' AND CommentDate<='%s'
		  AND SpecificVersion.svid=SpecificVersionComment.svid
		  AND SpecificVersion.svName='%s'"""%(time1,time2,svName)
    elif len(grade):
       query = """SELECT CommentDate,Comment 
		  FROM GraphPath,Version,SpecificVersion,SpecificVersionComment WHERE
		  CommentDate>='%s' AND CommentDate<='%s'
		  AND Version.grade='%s' AND Version.timeStamp='%s'
		  AND Version.graphid=GraphPath.graphid
		  AND GraphPath.svid=SpecificVersion.svid"""%(time1,time2,grade,timeS)
    if verbose:
       print string.join(string.split(query))
       
    cu.execute(query)
    print "### Between %s-%s the following comments found:"%(time1,time2)
    tup = cu.fetchall()
    finalList=[]
    for item in tup:
	cTime=item[0]
	msg  =item[1]
	if not finalList.count((cTime,msg)):
	   print cTime,msg
	   finalList.append((cTime,msg))

#
# main
#
if __name__ == "__main__":
    ESGetComment(sys.argv)

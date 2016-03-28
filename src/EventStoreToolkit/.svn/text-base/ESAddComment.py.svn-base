#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2005
#
"""The ESAddComment function allows add additional comments into EventStore."""

import os, sys, string, re, time
import es_init, esdb_auth, os_path_util
from es_init import ESInit, checkArg

def ESAddComment(args):
    """ESAddComment was designed to add comments into EventStore DB.
    To add your comment you need to provide old/new grade or data
    version name and old/new timeStamp. The comment can be added 
    either from command line or can be read from ASCII file.

    Please note, ESAddComment is a wrapper shell script around addComment.py
    module which does the work.
    """
    localOpt=["[ -grade <grade> -timeStamp <time> | -dataVersionName <name> ]"]
    localOpt.append("[ -date <date> ] [ -comment <someText> ]")
    
    usage=es_init.helpMsg("ESAddComment",localOpt)
    usageDescription="""
Option description:
    To add your comment to EventStore, either use grade/time or dataVersionName.
    """
    examples="""
    Examples:
    ESAddComment -grade physics -timeStamp 20090101 -comment Add new physics grade
    ESAddComment -dataVersionName PP2-20090101 -comment myComment.txt
    in last example we add comment from a myComment.txt file.
    """

    userCommand="addComment.py"
    optList, dictOpt = es_init.ESOptions(userCommand,args,usage,usageDescription,examples)
    dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
    historyFile,logFile,verbose,profile             = optList[1]
    userCommand                                     = optList[2]
    
    grade     =""
    cTime     = time.strftime("%Y%m%d",time.localtime())
    timeStamp =""
    x         = 1
    svName    = ""
    host      = ""
    verbose   = 0
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
	       checkArg([grade])
	       x+=2
	       continue
	    if args[x]=="-timeStamp":
	       timeStamp = args[x+1]
	       checkArg([timeStamp])
	       x+=2
	       continue
	    if args[x]=="-date":
	       cTime = args[x+1]
	       checkArg([cTime])
	       x+=2
	       continue
	    if args[x]=="-comment":
	       x+=1
	       while(args[x][0]!="-"):
		  if os.path.isfile(args[x]):
		     comment += open(args[x]).read()
		     break
		  comment += args[x]
		  x+=1
		  if len(args)==x:
		     break
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
     
    if len(cTime)!=8:
       print "Incorrect date=%s format found, please use YYYYMMDD"%cTime
       sys.exit(1)
	    
    # connect to MySQL EventStoreDB
    outputLog, globalLog = es_init.ESOutputLog(logFile) 
    db, dbType           = es_init.ESDBConnector(dbHost,dbName,userName,userPass,dbPort,dbSocket)
    pid                  = "%s"%os.getpid()
    localtime            = "%s"%time.strftime("%H:%M:%S",time.localtime())
    outputLog.write("\n%s %s ###### %s initialization is completed"%(pid,localtime,dbType))
    if dbType=="sqlite":
       addToQuery=""
    else:
       addToQuery=" FOR UPDATE"
    cu = db.cursor()

    listOfSVIDs=[]
    if not svName:
       query="""SELECT SpecificVersion.svid FROM Version,SpecificVersion,GraphPath WHERE
		timeStamp='%s' AND grade='%s' AND Version.graphid=GraphPath.graphid
		AND GraphPath.svid=SpecificVersion.svid
       """%(timeStamp,grade)
       if verbose:
	  print string.join(string.split(query))
       cu.execute(query)
       tup = cu.fetchall()
       if not len(tup) or not tup:
	  print "Upon your request, the following query return NULL results\n",query
	  print "Please check that provided grade/time or dataVersionName exists in ES"
	  sys.exit(1)
       for item in tup:
	   if not listOfSVIDs.count(item[0]):
	      listOfSVIDs.append(item[0])
    else:
       query="SELECT svid FROM SpecificVersion WHERE svName='%s'"%svName
       if verbose:
	  print string.join(string.split(query))
       cu.execute(query)
       tup = cu.fetchone()
       if not tup:
	  print "Upon your request, the following query return NULL results\n",query
	  print "Please check that provided grade/time or dataVersionName exists in ES"
	  sys.exit(1)
       listOfSVIDs.append(tup[0])
	    
    if not len(listOfSVIDs):
       print "No matches in ES DB found for your request"
       sys.exit(1)

    for svid in listOfSVIDs:
	modComment = string.replace(comment,"'","\\'")
	query="""INSERT INTO SpecificVersionComment (svid,CommentDate,Comment) 
		 VALUES('%s','%s','%s')"""%(svid,cTime,modComment)
	if verbose:
	  print string.join(string.split(query))
	if dbType=="mysql":
	   cu.execute("BEGIN")
	cu.execute(query)
	if dbType=="mysql":
	   cu.execute("COMMIT")
	else:
	   db.commit()

#
# main
#
if __name__ == "__main__":
    ESAddComment(sys.argv)

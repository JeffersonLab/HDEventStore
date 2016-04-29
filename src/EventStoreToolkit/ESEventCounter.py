#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004, 2005 
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
# This utility counts events for given run/grade/timeStamp in EventStore
"""ESEventCounter counts events for given run/grade/timeStamp in EventStore"""

import os, sys, string, re, time
import esdb_auth, os_path_util, es_init, gen_util, sql_util, key_dump
from es_init import ESInit, checkArg
import SOAPpy

# include python profiler only if python version greater then 2.3
if es_init.checkPythonVersion("2.3"):
   import hotshot			# Python profiler
   import hotshot.stats			# profiler statistics


##### FIX THIS   
class MetaData_Services:
    def __init__(self):
        CLEO_wsdl      = 'http://cougar.cs.cornell.edu/CLEO/CLEO_WS.asmx?WSDL'
        CLEO_namespace = 'http://cleo.lepp.cornell.edu/CLEO'
        config         = SOAPpy.SOAPConfig ()
        config.debug   = 0
        config.buildWithNamespacePrefix = False

        self.proxy = SOAPpy.WSDL.Proxy(CLEO_wsdl,config,namespace=CLEO_namespace,noroot=1)

        meths = self.proxy.methods.values()
        for m in meths:
            m.namespace = CLEO_namespace
            
    def GetRuns(self,datasetName,energy=''):
        dict  = self.proxy.GetRuns(Dataset_Name=datasetName,Energy_Range_Name=energy)
        returnList = dict.__dict__.items()
        # returnList consists of set of tuples:
        # type, keyVal, ns, name, typed, cache, data, keyord, attrs
        retType, runList = returnList[1]
        return runList
        
    def GetDatasetNames(self):
        dict  = self.proxy.GetDatasetNames()
        returnList = dict.__dict__.items()
        # returnList consists of set of tuples:
        # type, keyVal, ns, name, typed, cache, data, keyord, attrs
        retType, runList = returnList[1]
        return runList
        
    def GetEnergyRangeNames(self):
        dict  = self.proxy.GetEnergyRangeNames()
        returnList = dict.__dict__.items()
        # returnList consists of set of tuples:
        # type, keyVal, ns, name, typed, cache, data, keyord, attrs
        retType, runList = returnList[1]
        return runList
        
    def printSOAPMethods(self):
        print '%d methods in WSDL:' % len(self.proxy.methods) + '\n'
        for key in self.proxy.methods.keys():
           print key
        print
    
def countRecords(keyFileDict):
    sDict = {}
    runList = keyFileDict.keys()
    runList.sort()
    for run in runList:
        sList = []
        firstFile = ""
        for file in keyFileDict[run]:
            if not len(sList):
               sList = key_dump.countEvents(file)
               firstFile = file
            else:
               tmpList = key_dump.countEvents(file)
               for idx in xrange(0,len(sList)):
                   if sList[idx][0]!=tmpList[idx][0]:
                      msg = "records mismatch between:"
                      msg+= firstFile
                      msg+= sList
                      msg+= file
                      msg+= tmpList
                      raise msg
                   rec = sList[idx]
                   newRec = (rec[0],rec[1]+tmpList[idx][1])
                   sList[idx] = newRec
        msg   = "Run %s       :"%run
        counter=0
        for item in sList:
            stream = item[0]
            nRec   = item[1]
            if  inStream=="all":
                if not counter:
                   msg+=" %s in %s's\n"%(nRec,stream)
                else:
                   msg+="                   %s in %s's\n"%(nRec,stream)
                counter+=1
            elif stream==inStream:
                msg+=" %s in %s's"%(nRec,stream)
            if sDict.has_key(stream):
               total = sDict[stream]+nRec
               sDict[stream]=total
            else:
               sDict[stream]=nRec
        print msg
    print "----------------------------------------------"
    counter=0
    for key in sDict.keys():
        if  inStream=="all":
            if not counter:
               print "Total            : %s in %s's"%(sDict[key],key)
            else:
               print "                   %s in %s's"%(sDict[key],key)
            counter+=1
        elif inStream==key:
            print "Total            : %s in %s's"%(sDict[key],key)

def getKeyFileList(runList,graphid,view):
    # get list of key files which cover required run range
    keyFileList = []
    for run in runList:
        query = "SELECT keyFileId FROM KeyFile WHERE graphid='%s' AND run=%s AND view='%s'"%(graphid,run,view)
        tup   = sqlIn.fetchAll(query)
        if verbose:
           print query
           print tup
        for item in tup:
            query = "SELECT fileName FROM FileID WHERE fileId=%s"%item[0]
            tup   = sqlIn.fetchOne(query)
            if verbose:
               print query
               print tup
            if tup and tup[0]:
               keyFileList.append((run,tup[0]))
    return keyFileList

def getKeyFileDict(runList,graphid,view):
    keyFileDict = {}
    for run in runList:
        query = "SELECT keyFileId FROM KeyFile WHERE graphid='%s' AND run=%s AND view='%s'"%(graphid,run,view)
        tup   = sqlIn.fetchAll(query)
        for item in tup:
            query = "SELECT fileName FROM FileID WHERE fileId=%s"%item[0]
            tup   = sqlIn.fetchOne(query)
            if tup and tup[0]:
               if keyFileDict.has_key(run):
                  eList = keyFileDict[run]
                  eList+=[tup[0]]
                  keyFileDict[run]=eList
               else:
                  keyFileDict[run]=[tup[0]]
    return keyFileDict
#
# main
#
if __name__ == "__main__":

	# initialize user options
	localOpt=["[ -grade <grade> ]  [ -time <timeStamp> ] [ -skim <skim> ]"]
	localOpt+=["[ -runRange <minR> <maxR> -dataVersionName <svName> |"]
        localOpt+=["  -dataset <dataset> -energy <energyName> ]"]
	localOpt+=["[ -instream <streamName> ]"]
	localOpt.append("")
	
	usage=es_init.helpMsg("ESEventCounter",localOpt)
	usageDescription="""
Option description (required options marked with '*'):
*       -grade:     specifies the grade, e.g. "physics", "p2-unchecked"
                    you need to provide two of them, 'old' from which you're reading
		    and 'new' to which you're moving
*       -time:      specifies the timeStamp, e.g. 20090227
                    you may provide one or two of them, 'old' from which you're reading
		    and 'new' to which you're moving
	-dataVersionName 
	            specifies a data version name associated with given grade.
	            It can be found by using ESDump command.
	-runRange   specifies a run range within ESVersionManager will operate
        -dataset    specifies a data set name, e.g. data32
        -energy     specifies an enenrgy name, e.g. psi(2S)
        -skim       specifies the view you want to use, e.g. "2photon"
        -instream   specifies the stream you're interesting, e.g. -instream beginrun
                    once dropped ESEventCounter will count everything in every found stream
	"""
	examples="""
        ESEventCounter -db EventStore@lnx150.lns.cornell.edu -grade physics -time 20050520 -runRange 213038 213040 -view bhagam
        count events(records) in every stream for runs in 213038-213040 range for physics grade and
        time stamp closed to 20050520.
        
        ESEventCounter -db EventStore@lnx150.lns.cornell.edu -grade physics -time 20050520 -runRange 213038 213038 -instream event
        count only events (events in event stream) for run 213038, physics grade and time stamp
        closed to 20050520.
        
        ESEventCounter -db EventStore@lnx150.lns.cornell.edu -grade physics -time 20050520 -dataset data39 -instream event
        count only events (events in event stream) for runs from data39, physics grade and time stamp
        closed to 20050520.
        
        ESEventCounter -db EventStore@lnx150.lns.cornell.edu -grade physics -time 20050520 -energy "psi(2S)"
        count only events (events in event stream) for runs in psi(2S), physics grade and time stamp
        closed to 20050520.
	"""
	userCommand="ESEventCounter.py"
	
	# get EventStore option list
	optList,dictOpt = es_init.ESOptions(userCommand,sys.argv,usage,usageDescription,examples)
	dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
	historyFile,logFile,verbose,profile             = optList[1]
	userCommand                                     = optList[2]+" "
        userName = 'cleo'
        userPass = 'cleoc'
	outputLog, globalLog = es_init.ESOutputLog(logFile) 

        dbInHost=bInName=dbOutHost=dbOutHost=oldDir=newDir=""
	grade      = ""
	timeS      = ""
	minR       = 0
	maxR       = 0
	svName     = ""
        view       = "all"
        inStream   = "all"
        dataset    = ""
        energy     = ""
	badRunList = 0
	goodRunList= 0
	x          = 1
	fileRunList= ""
	while x < len(sys.argv):
	    try:
                if string.lower(sys.argv[x]) == "-db" : 
                   dbInName,dbInHost,dbInPort,dbInSocket=es_init.decodeHostNameString(sys.argv[x+1])
                   x+=2
                   continue
		if sys.argv[x]=="-runRange":
		   minR = int(sys.argv[x+1])
                   msg = "you need to use -runRange <minR> <maxR>"
                   if x+2<len(sys.argv) and sys.argv[x+2]:
                      if sys.argv[x+2][0]=="-":
                         raise msg
                   else:
                      raise msg
		   maxR = int(sys.argv[x+2])
		   checkArg([minR,maxR])
	           x+=3
		   continue
		if sys.argv[x]=="-grade":
		   grade = sys.argv[x+1]
		   checkArg([grade])
		   x+=2
		   continue
		if sys.argv[x]=="-dataset":
		   dataset = sys.argv[x+1]
		   checkArg([dataset])
		   x+=2
		   continue
		if sys.argv[x]=="-energy":
		   energy = sys.argv[x+1]
		   checkArg([energy])
		   x+=2
		   continue
		if sys.argv[x]=="-view" or sys.argv[x]=="-skim":
		   view = sys.argv[x+1]
		   checkArg([view])
		   x+=2
		   continue
		if sys.argv[x]=="-instream":
		   inStream = sys.argv[x+1]
		   checkArg([inStream])
		   x+=2
		   continue
		if sys.argv[x]=="-timeStamp" or sys.argv[x]=="-time":
		   timeS = sys.argv[x+1]
		   checkArg([timeS])
		   x+=2
		   continue
		if sys.argv[x]=="-dataVersionName":
		   svName = sys.argv[x+1]
		   checkArg([svName])
		   x+=2
		   continue
	        # if we reach here, that means we found unkown option
	        if dictOpt.has_key(sys.argv[x]):
		   x+=dictOpt[sys.argv[x]]
		else:
		   msg="Option '%s' is not allowed"%sys.argv[x]
		   raise msg
	    except:
                gen_util.printExcept()
		sys.exit(1)

	if not grade:
	   print "You need to provide a grade"
	   print usage
	   sys.exit(1)
	if not timeS:
	   print "You need to provide a time stamp"
	   print usage
	   sys.exit(1)
           
	pid       = "%s"%os.getpid()
	localtime = "%s"%time.strftime("%H:%M:%S",time.localtime())

        # connect to dbIn EventStore
        db,dbType = es_init.ESDBConnector(dbInHost,dbInName,userName,userPass,'EXCLUSIVE',dbInPort,dbInSocket)
	outputLog.write("\n%s %s ###### %s initialization is completed"%(pid,localtime,dbType))
        sqlIn     = sql_util.SQLUtil(db,dbType,outputLog)

        # get closest time stamp in DB
        query = "SELECT DISTINCT timeStamp FROM Version WHERE timeStamp<=%s AND grade='%s' ORDER BY timeStamp DESC"%(timeS,grade)
        tup   = sqlIn.fetchOne(query)
        realTime=0
        if tup and tup[0]:
           realTime = tup[0]
        else:
           print "Fail to execute '%s'"%query
           sys.exit(1)
        # get graphid's
        gidList = []
        svNameList = []
        if svName:
           query = """SELECT DISTINCT graphid FROM GraphPath,SpecificVersion WHERE svName='%s' AND
           GraphPath.svid=SpecificVersion.svid"""%svName
           tup   = sqlIn.fetchAll(query)
           if tup and tup[0]:
              graphid = tup[0]
           else:
              print "Fail to execute '%s'"%query
              sys.exit(1)
           svNameList.append(svName)
           gidList.append(graphid)
        else:
           query = "SELECT DISTINCT graphid FROM Version WHERE timeStamp=%s AND grade='%s' ORDER BY graphid DESC"%(realTime,grade)
           tup   = sqlIn.fetchAll(query)
           for item in tup:
               graphid = item[0]
               gidList.append(graphid)
               query = "SELECT svName FROM GraphPath,SpecificVersion WHERE graphid=%s AND GraphPath.svid=SpecificVersion.svid"%graphid
               res = sqlIn.fetchOne(query)
               if res and res[0]:
                  svNameList.append(res[0])
               else:
                  print "Fail to execute '%s'"%query
                  sys.exit(1)
        if  inStream=="all":
            print "Lookup EventStore: %s@%s:%s:%s"%(dbInHost,dbInName,dbInPort,dbInSocket)
            print "grade            : %s"%grade
            print "timeStamp        : %s"%realTime
            print "view             : %s"%view
            for idx in xrange(0,len(gidList)):
                print "dataVersionName  : %s"%svNameList[idx]
                print "graphid          : %s"%gidList[idx]

        metadataServices = MetaData_Services()
        # check if we got dataset request, if so identify minR/maxR
        if dataset or energy:
           runList = metadataServices.GetRuns(dataset,energy)
           minR = int(runList[0])
           maxR = int(runList[-1])

        # get full list of runs we need to cover
        for idx in xrange(0,len(gidList)):
            graphid= gidList[idx]
            svName = svNameList[idx]
            query = """SELECT minRunNumber,maxRunNumber FROM Version WHERE grade='%s' AND
                       timeStamp='%s' AND graphid='%s'"""%(grade,realTime,graphid)
            if minR:
               query+=" AND minRunNumber<=%s"%minR
            if maxR:
               query+=" AND maxRunNumber>=%s"%maxR
            tup = sqlIn.fetchAll(query)
            if verbose:
               print query
               print tup
            runList=[]
            for item in tup:
                for run in xrange( int(item[0]),int(item[1])+1 ):
                    if run>=minR and run<=maxR:
                       runList.append(run)
            if not len(runList): continue
            if verbose:
               print "minR",minR,"maxR",maxR,"runList",runList
            # get list of key files which cover required run range
            keyFileDict = getKeyFileDict(runList,graphid,view)
            if not len(keyFileDict.keys()):
               print "Runs in '%s': N/A\n"%svName
            else:
               print "Runs in '%s':"%svName
            countRecords(keyFileDict)

	 

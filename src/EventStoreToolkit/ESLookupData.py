#!/usr/bin/env python

import os, sys, string, time, glob, stat, shutil, traceback
import es_init, file_util, sql_util, gen_util
import file_util, hddm_r_dump, key_dump
from es_init import ESInit, checkArg

class ESLookupManager(ESInit):
    def __init__(self,db, dbType, logFile, gid):
        ESInit.__init__(self, db, dbType, logFile)
        self.gid = gid
        print "Init ESLookupManager gid=%s"%gid
    def lookupKeyFiles(self):
        q="""select fileName from FileID, KeyFile where FileID.fileid=KeyFile.keyfileid and graphid=%s"""%self.gid
        tup = self.fetchAll(q)
	for x in tup:
            print x[0]
    def lookupLocationFiles(self):
        q="""select fileName from FileID, Location where FileID.fileid=Location.locationfileid and graphid=%s"""%self.gid
        tup = self.fetchAll(q)
	for x in tup:
            print x[0]
            self.lookupDataFiles(x[0])
    def lookupDataFiles(self,lpds):
        fidList = lpds_dump.getFileIds(lpds)
        for idx in xrange(0,len(fidList),2):
            pdsID = (fidList[idx+1]<<32)|fidList[idx]
            q="""select fileName from FileID where fileid=%s"""%pdsID
            tup = self.fetchAll(q)
	    for x in tup:
                print x[0]
    def process(self):
        self.lookupKeyFiles()
        self.lookupLocationFiles()
        return self.ok
#
# main
#
if __name__ == "__main__":
	localOpt=["[ -graphid gid ]"]
	usage=es_init.helpMsg("ESLookupData",localOpt)
	usageDescription="""
Option description:
	"""
	examples   = ""
	userCommand="ESLookupData.py"
	optList, dictOpt = es_init.ESOptions(userCommand,sys.argv,usage,usageDescription)
	dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
	historyFile,logFile,verbose,profile             = optList[1]
	userCommand                                     = optList[2]
	# parse the rest of the options and form user's command
	x = 1
	counter = 0
	while x < len(sys.argv):
          try:
	     if sys.argv[x] == "-graphid":
		gid=sys.argv[x+1]
		x+=2
		continue
	     # if we reach here, that means we found unkown option
	     if dictOpt.has_key(sys.argv[x]):
		x+=dictOpt[sys.argv[x]]
	     else:
		print "Option '%s' is not allowed"%sys.argv[x]
                traceback.print_exc()
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
	mydb = ESLookupManager(db,dbType,outputLog,gid)
	status = mydb.process()
	rStatus = es_init.ESOutput(status,userCommand,historyFile,outputLog,globalLog)
	sys.exit(rStatus)


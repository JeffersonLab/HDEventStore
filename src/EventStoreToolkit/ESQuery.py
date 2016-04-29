#!/usr/bin/env python
#
# Author:  Sean Dobbs (s-dobbs@northwestern.edu), 2015
#
"""ESQuery displays information from the EventStore tables in a user friendly form"""

import os, sys, string, time
import es_init
from es_init import ESInit, checkArg

class ESQueryManager(ESInit):
    """Provides methods for requesting various types of information from the EventStore DB"""
    def __init__(self,db, dbType, logFile=""):
        ESInit.__init__(self, db, dbType, logFile)
        self.cu = db.cursor()
    def printGrades(self):
	"""Print list of all available grades of data"""
        query = "SELECT DISTINCT grade FROM Version WHERE state='active'"
        self.cu.execute(query)
        tup = self.cu.fetchall()
        print "Available grades:"
        print ""
        for item in tup:
           print "  " + str(item[0])
	return self.ok
    def printTimeStamps(self,grade):
	"""Print list of all available data timestamps"""
        query = "SELECT DISTINCT timeStamp FROM Version WHERE grade='%s' AND state='active'"%grade
        self.cu.execute(query)
        tup = self.cu.fetchall()
        print "Available timestamps:"
        print ""
        for item in tup:
           print "  " + str(item[0])
	return self.ok
    def printSkims(self,grade,timeStamp):
	"""Print list of all available skims for a given grade of data"""
        query = "SELECT MAX(timeStamp) FROM Version WHERE timeStamp<='%s' AND grade='%s' AND state='active'" % (timeStamp,grade)
        self.cu.execute(query)
        realTimeStamp = self.cu.fetchone()[0]
        #
        query = "SELECT DISTINCT view FROM Version,KeyFile WHERE timeStamp='%s' AND Version.grade='%s' AND Version.state='active' AND Version.graphid=KeyFile.graphid GROUP BY view" % (realTimeStamp,grade)
        self.cu.execute(query)
        tup = self.cu.fetchall()
        print "Available skims:"
        print ""
        for item in tup:
           print "  " + str(item[0])
	return self.ok
    def printActualDate(self,grade,timeStamp):
	"""Print data version timestamp in DB corresponding to given timestamp"""
        query = "SELECT timeStamp FROM Version WHERE timeStamp<='%s' AND grade='%s' AND state='active' ORDER BY timeStamp DESC" % (timeStamp,grade)
        self.cu.execute(query)
        # select the most recent timestamp corresponding the one passed to us
        realTimeStamp = self.cu.fetchone()[0]  
        #
        print "Requsted timestamp = %s  Actual timestamp = %s" % (timeStamp,realTimeStamp)
	return self.ok
    def printSpecificVersionRunRanges(self,grade,timeStamp):
	"""Print list of all versions and run ranges for given data grade"""
        query = "SELECT MAX(timeStamp) FROM Version WHERE timeStamp<='%s' AND grade='%s' AND state='active'" % (timeStamp,grade)
        self.cu.execute(query)
        realTimeStamp = self.cu.fetchone()[0]
        #
        query = "SELECT timeStamp,minRunNumber,maxRunNumber,svName FROM Version,SpecificVersion,GraphPath WHERE Version.timeStamp<='%s' AND Version.grade='%s' AND Version.state='active' AND Version.graphid=GraphPath.graphid AND GraphPath.svid=SpecificVersion.svid ORDER BY Version.timeStamp DESC, Version.minRunNumber ASC"%(timeStamp,grade)
        self.cu.execute(query)
        tup = self.cu.fetchall()
        closestDate = ""
        print ""
        for item in tup:
            if closestDate == "":
                closestDate = item[0]
            else:
                if closestDate != item[0]:
                    break
            print " specific version = %s   run range = %s - %s " %(item[3],item[1],item[2])
	return self.ok
    def printGraphRunRanges(self,grade,timeStamp):
	"""Print list of all graph ids and un ranges for given data grade"""
        query = "SELECT timeStamp,minRunNumber,maxRunNymber,graphid FROM Version WHERE timeStamp<=%s AND grade='%s' AND state='active' ORDER BY timeStamp DESC, minRunNumber ASC"%(timeStamp,grade)
        self.cu.execute(query)
        tup = self.cu.fetchall()
        closestDate = ""
        print ""
        for item in tup:
            if closestDate == "":
                closestDate = item[0]
            else:
                if closestDate != item[0]:
                    break
            print " graphId = %s   run range = %s - %s " %(item[3],item[1],item[2])
	return self.ok
    def printAvailableRuns(self,grade=None):
	"""Print list of available runs data"""
        print ""
        print "Available runs:"
        if grade is not None and grade != "all":
            query = "SELECT minRunNumber,maxRunNumber FROM Version WHERE grade='%s' AND state='active'"%grade
            self.cu.execute(query)
            tup = self.cu.fetchall()
            for item in tup:
                query = "SELECT run from RunUID WHERE run>=%s AND run<=%s"%(item[0],item[1])
                self.cu.execute(query)
                runs = self.cu.fetchall()
                for run in runs:
                    print run[0]
        else:
            query = "SELECT UNIQUE run FROM RunUID"
            self.cu.execute(query)
            tup = self.cu.fetchall()
            for item in tup:
                print item[0]
	return self.ok

    ### "advanced" commands for development purposes
    def printFID(self,filename):
        """print file ID corresponding to given file"""
        query = "SELECT DISTINCT fileId FROM FileID WHERE fileName='%s'"%filename
        self.cu.execute(query)
        fid = self.cu.fetchone()[0]
        #
        print " Filename = %s  FID = %s"%(filename,fid)
        return self.ok
    def printFilename(self,fid):
        """print file name corresponding to given file ID"""
        query = "SELECT fileName FROM FileID WHERE fileId='%d'"%fid
        self.cu.execute(query)
        filename = self.cu.fetchone()[0]
        #
        print " FID = %s  Filename = %s"%(fid,filename)
        return self.ok
    def printUids(self,graphid,run):
        """print UIDs that are in the DB for a given graph ID/run"""
        query = "SELECT DISTINCT uid FROM KeyFile WHERE run='%d' AND graphid='%d'"%(run,graphid)
        self.cu.execute(query)
        tup = self.cu.fetchall()
        print "UIDs:"
        print ""
        for item in tup:
           print "  " + str(item[0])
        return self.ok
    def printFilenameAndTypeID(self,fid):
        """print file name and type ID from a given file ID"""
        query = "SELECT fileName,typeId FROM FileID WHERE fileId='%d'"%fid
        self.cu.execute(query)
        tup = self.cu.fetchall()
        print "Filename/Type IDs"
        print ""
        for item in tup:
           print "  " + str(item[0]) + " " + str(item[1])
        return self.ok
    def printTypeID(self,typename):
        """print the type ID corresponding to a given file type"""
        query = "SELECT typeId FROM FileType WHERE type='%s'"%typename
        self.cu.execute(query)
        fid = self.cu.fetchone()[0]
        #
        print " Filename = %s  FID = %s"%(filename,fid)
        return self.ok
    def printIsRunInDB(self,run,uid):
        """print if a run/uid pair exists in the DB"""
        query = "SELECT run,uid FROM RunUID WHERE run='%d'"%run
        self.cu.execute(query)
        tup = self.cu.fetchall()
        #
        found = False
        for item in tup:
            newuid = int(item[1])
            if newuid == 0:
                found = True
                break
            if newuid == uid:
                found = True
                break

        if found:
            print "Found run in DB"
        else:
            print "Could not find run in DB"
        return self.ok
    def printRunList(self,runRange,graphId,view):
        """print list of runs for a given data view"""
        beginRun = runRange[0]
        endRun = runRange[1]
        query = "SELECT DISTINCT run FROM KeyFile WHERE run>='%d' AND run<='%d' AND graphid='%d' AND view='%s' ORDER BY run ASC"%(beginRun,endRun,graphId,view)
        self.cu.execute(query)
        tup = self.cu.fetchall()
        #
        print "Run list:"
        print ""
        for item in tup:
           print "  " + str(item[0])
        return self.ok
    def printRunUidList(self,runRange,graphId,view):
        """print list of run/uid pairs for a given data view"""
        beginRun = runRange[0]
        endRun = runRange[1]
        query = "SELECT DISTINCT run,uid FROM KeyFile WHERE run>='%d' AND run<='%d' AND graphid='%d' AND view='%s' ORDER BY run ASC"  %(beginRun,endRun,graphId,view)
        self.cu.execute(query)
        tup = self.cu.fetchall()
        #
        print "Run/uid list:"
        print ""
        for item in tup:
           print "  " + str(item[0]) + "  " + str(item[1])
        return self.ok
    def printKeyFileID(self,graphID,view,run,uid=0):
        """print the key file ids for a particular run"""
        query = "SELECT keyFileId FROM KeyFile WHERE graphid='%d' AND view='%s' AND run='%d'"%(graphID,view,run)
        if uid>0:
            query += " AND uid='%d'"%uid
        self.cu.execute(query)
        #
        print " KeyFileID = %s" % self.cu.fetchone()
        return self.ok
    def printKeyFilename(self,graphID,view,run,uid=0):
        """print the key file name for a particular run"""
        query = "SELECT fileName FROM FileID,KeyFile WHERE graphid='%d' AND view='%s' AND run='%d' AND FileID.fileId=KeyFile.keyFileId"%(graphID,view,run)
        if uid>0:
            query += " AND uid='%d'"%uid
        self.cu.execute(query)
        #
        print " Key filename = %s" % self.cu.fetchone()
        return self.ok
        

#
# main
#
if __name__ == "__main__":
#	localOpt=["[ -noHeader ][ -runList <minRun> <maxRun> ] [ -time <YYYYDDMM> ]"]
	localOpt=["[ -noHeader ]"]
	usage=es_init.helpMsg("ESQuery",localOpt)
	usageDescription="""
ESQuery provides a user-friendly interface to get information from EventStore databases.

Command list with brief description:

  grades                                   - list of grades in DB
  timestamps <grade>                       - list of timestamps for <grade>
  skims <grade> [-time <YYYYDDMM>]         - list of skims for <grade>
  runs <grades>                            - list of runs for <grade>
  actualDate <grade> [-time <YYYYDDMM>]    - more recent date in DB to given time
  verions <grade> [-time <YYYYDDMM>]       - data version info for <grade>
  graphVerions <grade> [-time <YYYYDDMM>]  - graph version info for <grade>


Development commands:

  fid <filename>                             - file ID for <filename>
  fileName <fid>                             - file name for <fid>
  uids <run> <graphid>                       - UIDs stored for <run> in graph <graphid>
  fileNameType <fid>                         - file name and type <fid>
  typeID <typename>                          - ID for <typename>
  runInDB <run> <uid>                        - is this run in the DB?
  runList <brun> <erun> <graphid> [view]     - information for runs <brun> - <erun>
  runUIDList <brun> <erun> <graphid> [view]  - information for runs <brun> - <erun>
	"""

	examples   = ""
	userCommand="ESQuery.py"
	optList, dictOpt = es_init.ESOptions(userCommand,sys.argv,usage,usageDescription)
	dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
	historyFile,logFile,verbose,profile             = optList[1]
	userCommand                                     = optList[2]

	timeStamp= time.strftime("%Y%m%d",time.localtime())
	runs     = 0
	find     = 0
	x        = 1
        cmd      = ""
        noHeader = False
	while x < len(sys.argv):
           try:
              if sys.argv[x] == "-runList" : 
                 info = 0
                 runs = 1
                 minRun = sys.argv[x+1]
                 maxRun = sys.argv[x+2]
                 checkArg([minRun,maxRun])
                 x+=3
                 continue
              if sys.argv[x] == "-time" : 
                 timeStamp = sys.argv[x+1]
                 checkArg([timeStamp])
                 x+=2
                 continue
              if sys.argv[x] == "-noHeader" : 
                 noHeader = True
                 x+=1
                 continue
              # if we reach here, that means we found unkown option
              if dictOpt.has_key(sys.argv[x]):
                 x+=dictOpt[sys.argv[x]]
              else:
                 # only keep on trying to process options if we see an entry started with a '-'
                 if sys.argv[x][0] == '-':
                    print "Option '%s' is not allowed"%sys.argv[x]
                    raise
                 else:
                    break  ## finally done processing options
           except:
              sys.exit(1)

	# connect to DB
        #outputLog, globalLog = es_init.ESOutputLog(logFile) 
	db, dbType           = es_init.ESDBConnector(dbHost,dbName,userName,userPass,'',dbPort,dbSocket)
        if not noHeader:
           msg = "-"*(len(dbName)+3+len(dbHost))
           print " %s "%msg
           print "| %s@%s |"%(dbName,dbHost)
           print " %s "%msg
           print

	# create instance of ESDumpManager class
        #mydb = ESQueryManager(db,dbType,outputLog)
	mydb = ESQueryManager(db,dbType)

        # parse different command options
        cmd = sys.argv[x]

	if cmd == "grades":
	   status=mydb.printGrades()
        elif cmd == "timestamps":
           if (x+1) >= len(sys.argv):
              print "need to specify grade!"
              sys.exit(1)
           grade = sys.argv[x+1]
	   status=mydb.printTimeStamps(grade)
        elif cmd == "skims":
           if (x+1) >= len(sys.argv):
              print "need to specify grade!"
              sys.exit(1)
           grade = sys.argv[x+1]
	   status=mydb.printSkims(grade,timeStamp)
        elif cmd == "actualDate":
           if (x+1) >= len(sys.argv):
              print "need to specify grade!"
              sys.exit(1)
           grade = sys.argv[x+1]
	   status=mydb.printActualDate(grade,timeStamp)
        elif cmd == "runs":
           if (x+1) >= len(sys.argv):
              print "need to specify grade!"
              sys.exit(1)
           grade = sys.argv[x+1]
	   #status=mydb.printAvailableRuns(grade,timeStamp)  # use timestamp as well?
	   status=mydb.printAvailableRuns(grade)
        elif cmd == "versions":
           if (x+1) >= len(sys.argv):
              print "need to specify grade!"
              sys.exit(1)
           grade = sys.argv[x+1]
	   status=mydb.printSpecificVersionRunRanges(grade,timeStamp)
        elif cmd == "graphVersions":
           if (x+1) >= len(sys.argv):
              print "need to specify grade!"
              sys.exit(1)
           grade = sys.argv[x+1]
	   status=mydb.printGraphRunRanges(grade,timeStamp)

        ### development commands
        elif cmd == "fid":
           if (x+1) >= len(sys.argv):
              print "need to specify filename!"
              sys.exit(1)
           filename = sys.argv[x+1]
	   status=mydb.printFID(filename)
        elif cmd == "fileName":
           if (x+1) >= len(sys.argv):
              print "need to specify FID!"
              sys.exit(1)
           fid = int(sys.argv[x+1])
	   status=mydb.printFilename(fid)
        elif cmd == "uids":
           if (x+1) >= len(sys.argv):
              print "need to specify run!"
              sys.exit(1)
           if (x+2) >= len(sys.argv):
              print "need to specify graphid!"
              sys.exit(1)
           run = int(sys.argv[x+1])
           graphid = int(sys.argv[x+2])
	   status=mydb.printUids(run,graphid)
        elif cmd == "fileNameType":
           if (x+1) >= len(sys.argv):
              print "need to specify FID!"
              sys.exit(1)
           fid = int(sys.argv[x+1])
	   status=mydb.printFilenameAndTypeID(fid)
        elif cmd == "typeID":
           if (x+1) >= len(sys.argv):
              print "need to specify filename!"
              sys.exit(1)
           typename = sys.argv[x+1]
	   status=mydb.printTypeID(typename)
        elif cmd == "runInDB":
           if (x+1) >= len(sys.argv):
              print "need to specify run!"
              sys.exit(1)
           if (x+2) >= len(sys.argv):
              print "need to specify uid!"
              sys.exit(1)
           run = int(sys.argv[x+1])
           uid = int(sys.argv[x+2])
	   status=mydb.printIsRunInDB(run,uid)
        elif cmd == "runList":
           if (x+1) >= len(sys.argv):
              print "need to specify starting run!"
              sys.exit(1)
           if (x+2) >= len(sys.argv):
              print "need to specify ending run!"
              sys.exit(1)
           if (x+3) >= len(sys.argv):
              print "need to specify ending graph id!"
              sys.exit(1)
           if (x+4) >= len(sys.argv):
              print "need to specify ending view!"
              sys.exit(1)
           beginrun = int(sys.argv[x+1])
           endrun = int(sys.argv[x+2])
           graphid = int(sys.argv[x+3])
           view = sys.argv[x+4]
	   status=mydb.printRunList((beginrun,endrun),graphid,view)
        elif cmd == "runUIDList":
           if (x+1) >= len(sys.argv):
              print "need to specify starting run!"
              sys.exit(1)
           if (x+2) >= len(sys.argv):
              print "need to specify ending run!"
              sys.exit(1)
           if (x+3) >= len(sys.argv):
              print "need to specify ending graph id!"
              sys.exit(1)
           if (x+4) >= len(sys.argv):
              print "need to specify ending view!"
              sys.exit(1)
           beginrun = int(sys.argv[x+1])
           endrun = int(sys.argv[x+2])
           graphid = int(sys.argv[x+3])
           view = sys.argv[x+4]
	   status=mydb.printRunUidList((beginrun,endrun),graphid,view)
        elif cmd == "keyFileID":
           if (x+1) >= len(sys.argv):
              print "need to specify graph ID!"
              sys.exit(1)
           if (x+2) >= len(sys.argv):
              print "need to specify view!"
              sys.exit(1)
           if (x+3) >= len(sys.argv):
              print "need to specify run!"
              sys.exit(1)

           graphid = int(sys.argv[x+1])
           view = sys.argv[x+2]
           run = int(sys.argv[x+3])

           # uid is optional argument
           if (x+3) == len(sys.argv):
               uid = int(sys.argv[x+4])
               status=mydb.printKeyFileID(graphid,view,run,uid)
           else:
               status=mydb.printKeyFileID(graphid,view,run)
        elif cmd == "keyFileName":
           if (x+1) >= len(sys.argv):
              print "need to specify graph ID!"
              sys.exit(1)
           if (x+2) >= len(sys.argv):
              print "need to specify view!"
              sys.exit(1)
           if (x+3) >= len(sys.argv):
              print "need to specify run!"
              sys.exit(1)

           graphid = int(sys.argv[x+1])
           view = sys.argv[x+2]
           run = int(sys.argv[x+3])

           # uid is optional argument
           if (x+3) == len(sys.argv):
               uid = int(sys.argv[x+4])
               status=mydb.printKeyFilename(graphid,view,run,uid)
           else:
               status=mydb.printKeyFilename(graphid,view,run)
        else:
           print "unknown command: %s"%cmd

        print ""

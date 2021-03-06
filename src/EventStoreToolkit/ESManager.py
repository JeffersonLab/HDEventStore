#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#

"""Main class which build key/loc files and update EventStore tables in
MySQL/SQLite databases.

The following update methods are supported:
   - update DB using data file or set of files, we support either fileName, 
   file pattern or directory name input
   - update DB using IDXA (event list) file
   
   Data can be entered to EventStore using the following critireas:
   - run is not yet present in DB for given grade
   - data files shouldn't have overlaping data and sync values
     - data may overlap but have different sync values
     - data may have the same set of sync values but not overlaping proxies
   The same run can be injected only if:
     - data not overlap (e.g. adding D-tagging to pass2)
     - new view is assigned
"""

import os, sys, stat, string, array, shutil, time

# import auxilary modules for key/location files
#import build_key_from_hddm, build_hddm_location, hddm_utils, hddm_reader, hddm_dump  ##,lhddm_dump
#import build_binary_key_loc, binary_reader
import hddm_r_dump, build_key_from_hddm_r
import evio_dump, build_key_from_evio
import file_util, key_dump, es_init, sql_util
import gen_util, es_logger, idxa_reader
from es_init import ESInit

MAGIC_NUMBER=2**32

class ESManager(ESInit):
    """Main class which build key/loc files and update EventStore tables in MySQL/SQLite databases."""
    def __init__(self, db, dbType, logFile, verbose=0):
        ESInit.__init__(self, db, dbType, logFile)
	self.newdb  = 0
	self.parents= []
	self.grade  = "user-unchecked"
	self.timeS  = gen_util.dayAhead()
	self.svName = "ESTest"
	self.oDir   = "/tmp"
	self.view   = "all"
	self.minRun = 0
	self.maxRun = 1000000
	self.dupRead= ""
        # cache
        self.dictSVName      = {} # cache {svName:graphid}
        self.dictFileType    = {} # cache {fileName:fileType}
        self.idxaDict        = {} # cache {fileName:idxaReader}
        self.cacheFileContent= {} # cache {fileName:'[fileParserContent]'}
        self.cacheRunList    = {} # cache {fileName:[runList]}
        self.masterCache     = {}
        self.masterMaxId     = 1
#        self.ilog = es_logger.ESLogger('ESManager')
	self.skim  = 0
	self.noskim= 0

        # underlying DB info
	self.dbHost   = ""
	self.dbName   = ""
        self.dbPort   = ""
        self.dbSocket = ""
	
        # master DB info
	self.masterDBHost = ""
	self.masterDBName = ""
        self.masterDBPort = ""
        self.masterSocket = ""
        
    def setDBHost(self,dbHost):
        """Set EventStore host name.
        @type dbHost: string
        @param dbHost: user DB hostname
        @rtype: none
        @return: none
        """
        self.dbHost=dbHost
        
    def setDBName(self,dbName):
        """Set EventStore name.
        @type dbName: string
        @param dbName: user DB name
        @rtype: none
        @return: none
        """
        self.dbName=dbName
        
    def setDBPort(self,dbPort):
        """Set EventStore port
        @type dbPort: string
        @param dbPort: user DB name
        @rtype: none
        @return: none
        """
        if dbPort:
           self.dbPort=int(dbPort)
        
    def setDBSocket(self,dbSocket):
        """Set EventStore socket
        @type dbSocket: string
        @param dbSocket: user DB socket
        @rtype: none
        @return: none
        """
        self.dbSocket=dbSocket
        
    def setMasterDB(self,dbName,dbHost,dbPort="",dbSocket=""):
        """Set EventStore and host names of the underlying master ESDB.
        @type dbName: string
        @type dbHost: string
        @type dbPort: integer
        @type dbSocket: string
        @param dbName: user DB hostname
        @param dbHost: user DB hostname
        @param dbPort: db port, e.g. 3306 is default for MySQL
        @param dbSocket: socket file, e.g /var/log/mysql
        @rtype: none
        @return: none
        """
        self.masterDBName=dbName
        if dbPort: self.masterDBPort = int(dbPort)
        if dbSocket: self.masterSocket = dbSocket
        if dbHost:
	   self.masterDBHost = dbHost
        else:
	   self.masterDBHost = "esdb.research.northwestern.edu"
#        if string.find(os.environ['HOSTNAME'],"research.northwestern.edu")!=-1 and not dbHost:
#        else:
        
    def setGenerateDB(self,newdb):
        """Set a flag to generate new database.
        @type newdb: string or integer
        @param newdb: flag to inform ESManager to generate new DB
        @rtype: none
        @return: none
        """
        self.newdb=newdb
    def setParents(self,parents):
        """Set list of parents
        @type parents: list
        @param parents: list of parents
        @rtype: none
        @return: none
        """
        self.parents=parents
    def setGrade(self,grade):
        """Set grade name
        @type grade: string
        @param grade: name of the grade, e.g. 'physics'
        @rtype: none
        @return: none
        """
        self.grade=grade
    def setTimeStamp(self,timeS):
        """Set time stamp
        @type timeS: string or integer
        @param timeS: time stamp to be used, e.g. 20090909 
        @rtype: none
        @return: none
        if timeS==-1 we use L{gen_util.dayAhead} to set up a day ahead, otherwise
        we assign self.timeS=timeS
        """
	if timeS==-1:
	   timeS=gen_util.dayAhead()
        self.timeS=int(timeS)
    def setSVName(self,svName):
        """Set data version name
        @type svName: string
        @param svName: data version name, a.k.a specific version name
        @rtype: none
        @return: none
        """
        self.svName=svName
    def setOutputDir(self,oDir):
        """Set location of output directory which would be used to write out key/location files
        @type oDir: string
        @param oDir: name of output directory
        @rtype: none
        @return: none
        """
        self.oDir=oDir
    def setView(self,view):
        """Set view name
        @type view: string
        @param view: view name, e.g. 'qcd'
        @rtype: none
        @return: none
        """
        self.view=view
    def setMinRun(self,minR):
        """Set lower bound on run range
        @type minR: integer
        @param minR: minimum run number of run range
        @rtype: none
        @return: none
        """
        self.minRun=int(minR)
    def setMaxRun(self,maxR):
        """Set upper bound on run range
        @type maxR: integer
        @param maxR: maximum run number of run range
        @rtype: none
        @return: none
        """
        self.maxRun=int(maxR)
    def setReadDuplicatesSource(self,dupRead):
        """Set file name which will be used to resolve data overlap conflicts
        @type dupRead: string
        @param dupRead: file name
        @rtype: none
        @return: none
        """
        self.dupRead=dupRead
    def setSkimFlag(self,skim):
        """Set skim flag, i.e. inform ESManager to treat input sources as a skim
        @type skim: string or integer
        @param skim: inform ESManager that input source is a skim
        @rtype: none
        @return: none
        """
        self.skim=skim
    def setNoSkimFlag(self,noskim):
        """Set no-skim flag, i.e. inform ESManager to treat input sources as is
        @type noskim: string or integer
        @param noskim: inform ESManager to use input source as is
        @rtype: none
        @return: none
        """
        self.noskim=noskim
    #
    # generate key/location file
    #
    def genFileName(self,fileName,fileId,buildType):
        """Generate a unique key/location file name. 
	File name is formed by base of data file name,
	plus an unique file id assgined by FileID table during file id allocation.
        @type fileName: string
        @type fileId: integer
        @type buildType: string
        @param fileName: name of the data file, e.g. run111111.bin or myName.pds.
        File type is analyzed by L{file_util.fileType}.
        @param fileId: file id
        @param buildType: we may be ask to construct either 'location' or 'key' file names
        @rtype: string
        @return: file name in the following form: run-runNumber-esdb-fileId.extension.
        """
        tup = os.path.split(fileName)
	if len(tup[0]):
	   dirName = os.path.normpath(tup[0])+"/"
	else:
	   dirName = os.path.normpath(os.getcwd())+"/"
	if len(self.oDir): dirName=self.oDir
	fName    = tup[1]
	fileType = file_util.fileType(fileName)
	extension=""
        if buildType=="location":
	   extension="l"+fileType
        elif buildType=="key":
           extension="ikey"
        else:
           print "Wrong buildType=%s"%buildType
        #ext = "-esdb-%d.%s"%(fileId,extension)
	ext = "-esdb.%s"%(extension)
    	file = dirName+string.split(fName,"."+fileType)[0]+ext
	return file
    def genFile(self,fileName,fileId,loc=0):
        """Generate key/location files names. It uses genFileName to do a job.
        @type fileName: string
        @type fileId: integer
        @type loc: integer
        @param fileName: name of the file
        @param fileId: file id
        @param loc: flag to generate location file.
        @rtype: string
        @return: file name generated by L{genFleName} and files are generated either
        by L{file_util.build_location} for location files or
        L{file_util.build_key} for key files.
        """
        if loc:
    	   # generate location file
	   file = self.genFileName(fileName,fileId,"location")
    	   file_util.build_location(fileName,loc,file)
        else:
    	   # generate keyFile
	   file = self.genFileName(fileName,fileId,"key")
    	   file_util.build_key(fileName,file,int(fileId))
    	print file
	return file
    #
    # search for valid id in FileID
    #
    def getNextId(self,howMany):
        """Allocate a new file id in FileID table. We rely on autoincrement feature
	of underlying DB.
        @type howMany: integer
        @param howMany: generate 'howMany' new Id's in FileID table
        @rtype: list
        @return: list of unique id's generated by autoincrment while executing
        'INSERT INTO FileID(fileName,typeId) VALUES(NULL,0)' query.
        Id's are obtained by using L{getLastId} method. Query is logged by
        L{updateDBAndLog} method.
        """
        self.startTxn("getNextId")
        idList= []
        for i in xrange(0,howMany):
            query = """INSERT INTO FileID(fileName,typeId) VALUES(NULL,0)"""
            self.updateDBAndLog(query)
	    id = self.getLastId("FileID")
	    idList.append(id)
        self.endTxn("getNextId")
        return idList

    def getIds(self,howMany):
        """High-level function to retieve new set of unique ids. It uses getNextId to do a job.
        @type howMany: integer
        @param howMany: generate 'howMany' new Id's in FileID table
        @rtype: list
        @return: list of unique id's generated by L{getNextId}
        """
        idList = self.getNextId(howMany)
	if len(idList)!=howMany:
#           if self.dbType!="sqlite": self.db.rollback()
	   self.db.rollback()
	   self.writeToLog("Job aborted")
	   sys.exit(-1)
	if howMany==1: return idList[0]
	return idList
    #
    # print content of entire DB
    #
    def printESDBContent(self, dbid):
        """Prints content of EventStore based on its id.
        @type dbid: integer
        @param dbid: DB id
        @rtype: none
        @return: none
        """
	try:
	   dbname = self.dbNames[dbid]
	   self.printDBContent(dbname)
	except:
	   pass # in the case of new DB, we will fail to know dbid in advance (see sql_util ctor)
    def printAllDB(self):
        """Prints content of all EventStore table by using L{printDBContent}
        @rtype: none
        @return: none
        """
	for dbid in self.dbid:
	    self.printESDBContent(dbid)
    def printMsg(self,fileList,Message,level="ERROR"):
        """Form a general report message with outline of current DB shapshot.
        @type fileList: list
        @type Message: string
        @type level: string
        @param fileList: list of files
        @param Message: message which passed to here
        @param level: level of severity
        @rtype: none
        @return: none
        """
	print "\n"
	print "===> ",
	print "Snapshot of your request and DB status at this instance"
	print "grade          :",self.grade
	print "timeStamp      :",self.timeS
	print "specificVersion:",self.svName
	print "view           :",self.view
	print
        print "while processing"
	for f in fileList: print f
	print
	print "EventStore DB contains"
	self.printESDBContent(self.idx("Version"))
	print
	self.printESDBContent(self.idx("SpecificVersion"))
	print
	print level
	print Message
    # 
    # open/close DB(s)
    #
    def openDBs(self):
        """Open EventStore tables. If necessary use L{createTables} routine to
	create those. Everything is wrapped in transaction: L{startTxn} and L{endTxn}.
        """
	tables   = self.dbNames
	self.startTxn("openDBs")
        # if class initialized with newDB flag we need to create tables
        if self.newdb or not tables: self.createTables()
        self.endTxn("openDBs")
	# loop over all tables and see if anyone need to be created.
	for table in tables:
	    query= "SELECT * FROM %s LIMIT 1"%table
	    try:
	       tup  = self.fetchOne(query)
	    except:
	       # we need to create a table
	       if self.verbose: 
	          print "Creating a table",table
	       self.startTxn("openDBs")
	       self.createTables(table)
               self.endTxn("openDBs")
	       pass
    # 
    # update DBs
    #
    def updateFileID(self, id, name, typeId):
        """Update FileID table entry for given id, name and typeId. 
        We use L{startTxn} and L{endTxn} as transaction wrappers and L{updateDBAndLog}
        for table update and loging.
        @type id: long
        @type name: string
        @type typeId: integer
        @param id: file id
        @param name: file name
        @param typeId: type Id
        @rtype: none
        @return: none
        """
	self.writeToLog("updateFileID")
	query="SELECT fileId,fileName FROM FileID WHERE fileId='%s'"%id
	tup  = self.fetchOne(query)
	if  tup and tup[0]:
            fileId=tup[0]
            fileName=tup[1]
            if  fileName and fileName!=name:
                msg = "\n\nERROR: updateFileID\n"
                msg+= "Found (%s,%s)\n"%(fileId,fileName)
                msg+= "But recieved request to update this record with\n"
                msg+= name+"\n"
                msg+= "Operation is not allowed\n"
                print msg
                raise "fail to update FileID table"
            else:
                query ="""UPDATE FileID SET fileName='%s',typeId=%s
                          WHERE fileId=%s"""%(name,typeId,id)
                cQuery="DELETE FROM FileID WHERE fileId=%s"%id
                self.startTxn("updateFileID")
                self.updateDBAndLog(query,cQuery)
                self.endTxn("updateFileID")
	else:
	    query="INSERT INTO FileID (fileId,fileName,typeId) VALUES('%s','%s','%s')"%(id,name,typeId)
	    self.startTxn("updateFileID")
	    self.updateDBAndLog(query)
	    self.endTxn("updateFileID")
                
    def updateRunUID(self, run, uid):
        """Update RunID table.
        We use L{startTxn} and L{endTxn} as transaction wrappers and L{updateDBAndLog}
        for table update and loging.
        @type run: integer
        @type uid: long
        @param run: run number
        @param uid: unique id
        @rtype: none
        @return: none
        """
	self.writeToLog("updateRunUID")
        self.startTxn("updateRunUID")
        query="SELECT run FROM RunUID WHERE run=%s AND uid=%s"%(run,uid)
	tup  = self.fetchOne(query)
	if not tup:
           query="INSERT INTO RunUID (run,uid) VALUES (%s,%s)"%(run,uid)
	   cQuery="DELETE FROM RunUID WHERE run=%s AND uid=%s"%(run,uid)
	   self.updateDBAndLog(query,cQuery)
	self.endTxn("updateRunUID")

    def updateKeyFile(self,graphid,view,run,uid,key_id):
        """Update KeyFile table.
        We use L{startTxn} and L{endTxn} as transaction wrappers and L{updateDBAndLog}
        for table update and loging.
        @type graphid: integer
        @type view: string
        @type run: integer
        @type uid: long
        @type key_id: long
        @param graphid: graph id
        @param view: view, e.g. 'qcd'
        @param run: run number
        @param uid: unique id
        @param key_id: key file id
        @rtype: none
        @return: none
        """
	self.writeToLog("updateKeyFile")
	query="""INSERT INTO KeyFile (graphid,view,run,uid,keyFileId)
		 VALUES (%s,'%s',%s,%s,%s)"""%(graphid,view,run,uid,key_id)
	cQuery="DELETE FROM KeyFile WHERE keyFileId=%s"%key_id
	self.startTxn("updateKeyFile")
	self.updateDBAndLog(query,cQuery)
	self.endTxn("updateKeyFile")

    def updateLocation(self,graphid,run,uid,loc_id):
        """Update Location table.
        We use L{startTxn} and L{endTxn} as transaction wrappers and L{updateDBAndLog}
        for table update and loging.
        @type graphid: integer
        @type run: integer
        @type uid: long
        @type loc_id: long
        @param graphid: graph id
        @param run: run number
        @param uid: unique id
        @param loc_id: location file id
        @rtype: none
        @return: none
        """
        print "In updateLocation()..."
        print "Not using location files at the moment!"
        return

	self.writeToLog("updateLocation")
	query="""INSERT INTO Location (graphid,run,uid,locationFileId)
		 VALUES (%s,%s,%s,%s)"""%(graphid,run,uid,loc_id)
	cQuery="""DELETE FROM Location WHERE graphid='%s' AND run='%s' 
	          AND uid='%s' AND locationFileId='%s'"""%(graphid,run,uid,loc_id)
	self.startTxn("updateLocation")
	self.updateDBAndLog(query,cQuery)
	self.endTxn("updateLocation")

    def updateSpecificVersion(self,svName,svid):
        """Update SpecificVersion table.
        We use L{startTxn} and L{endTxn} as transaction wrappers and L{updateDBAndLog}
        for table update and loging.
        @type svName: string
        @type svid: integer
        @param svName: data version name, a.k.a specific version name
        @param svid: specific version id
        @rtype: none
        @return: none
        """
	self.writeToLog("updateSpecificVersion")
	query="""INSERT INTO SpecificVersion (svName,svid) 
		 VALUES ('%s',%s)"""%(svName,svid)
	cQuery="DELETE FROM SpecificVersion WHERE svName='%s'"%svName
	self.startTxn("updateSpecificVersion")
	self.updateDBAndLog(query,cQuery)
	self.endTxn("updateSpecificVersion")

    def updateGraphPath(self,graphid,svid):
        """Update GraphPath table.
        We use L{startTxn} and L{endTxn} as transaction wrappers and L{updateDBAndLog}
        for table update and loging.
        @type graphid: integer
        @param graphid: graph id
        @type svid: integer
        @param svid: specific version id
        @rtype: none
        @return: none
        """
	self.writeToLog("updateGraphPath")
	query="""INSERT INTO GraphPath (graphid,svid) 
		 VALUES (%s,%s)"""%(graphid,svid)
	cQuery="DELETE FROM GraphPath WHERE graphid='%s'"%graphid
	self.startTxn("updateGraphPath")
	self.updateDBAndLog(query,cQuery)
	self.endTxn("updateGraphPath")

    def updateFileType(self, fileName):
        """Update FileType table. File type is determined by L{file_util.fileType}.
        We use L{startTxn} and L{endTxn} as transaction wrappers and L{updateDBAndLog}
        for table update and loging.
        @type fileName: string
        @param fileName: name of the file
        @rtype: integer
        @return: newly allocated or obtained file type id
        """
	self.writeToLog("updateFileType")
        type = file_util.fileType(fileName)
	desc = "Unknown"
	if   type =="ikey": desc="Index key file"
        elif type =="evio": desc="EVIO data file"
	elif type =="hddm": desc="HDDM data file"  
	elif type =="rest": desc="REST data file"  
	elif type =="mc":   desc="MC data file"  
	
	# first check out fileType dictionary and return id if type is found
	if self.dictFileType.has_key(type):
	   return self.dictFileType[type]

	query = "SELECT id,type FROM FileType"
	tup   = self.fetchAll(query)
	for x in tup:
	    if type==x[1]:
	       return x[0]
	self.startTxn("updateFileType")
	query="""INSERT INTO FileType (type,description)
		 VALUES ('%s', '%s')"""%(type,desc)
	self.updateDBAndLog(query)
	id = self.getLastId("FileType")
	self.endTxn("updateFileType")
	# put this type to fileType dictionary
	self.dictFileType[type]=id
#        if self.verbose: 
#           print "For %s type=%s id=%s description=%s"%(fileName,type,id,desc)
	return id
	
    def getFileID(self,fileName):
        """Lookup in FileID and either return fileID or 0 if file is not present.
        We use L{fetchOne} method to make a query.
        @type fileName: string
        @param fileName: name of the file
        @rtype: string (we use type of returning query)
        @return: file id or 0
        """
	query="SELECT fileId FROM FileID WHERE fileName='%s'"%fileName
	tup = self.fetchOne(query)
	if tup:
	   return tup[0]
	return 0
	
    def compareLists(self,list1,list2):
        """Compare two list for their intersection. If use python 2.4 we use 'set'
	intersection module, otherwise we count every entry from one list into another.
        @type list1: list
        @type list2: list
        @param list1: list
        @param list2: list
        @rtype: integer
        @return: 1 if lists overlap and 0 otherwise. For python 2.4 and above we use
        set(list1) & set(list2), otherwise loop over list1 and search its entries in list2.
        """
	useSetOK    = 0
        if es_init.checkPythonVersion("2.4"):
	   useSetOK = 1
	overlap = 0
	if useSetOK:
	   if set(list1) & set(list2):
	      overlap = 1
	else:
	   for item in list1:
	       if list2.count(item):
		  overlap = 1
		  break
	return overlap
    
    def requestDataFromUserDB(self,query,whatToRetrieve="all"):
        """We may request data either from user or master DBs. Return tuple for given query.
        @type query: string
        @type whatToRetrieve: string
        @param query: SQL query
        @param whatToRetrieve: a keyword to distinguish what to retrieve, e.g. 'all' or 'one'.
        @rtype: none
        @return: tuple
        We use either L{fetchOne} or L{fetchAll} methods for quering.
        """
        if whatToRetrieve=="all":
	   userTuple = self.fetchAll(query)
        else:
	   userTuple = self.fetchOne(query)
	return userTuple
        
    def requestDataFromDB(self,query,whatToRetrieve="all"):
        """Send query to the master DB, the master DB may be specified by user, otherwise 
        use EventStore@lnx151.
        @type query: string
        @type whatToRetrieve: string
        @param query: SQL query
        @param whatToRetrieve: a keyword to distinguish what to retrieve, e.g. 'all' or 'one'.
        @rtype: none
        @return: tuple
        We use either L{es_init.requestDataFromDB} methods for quering.
        """
        # check cache if we have master DB connector
	mkey  = (self.masterDBName,self.masterDBHost,self.masterDBPort,self.masterSocket) 
        db = dbType = cu = ""
        if self.masterCache.has_key(mkey):
           db,dbType = self.masterCache[mkey]
        else:
           db,dbType = es_init.connectToMasterDB(self.masterDBName,self.masterDBHost,self.masterDBPort,self.masterSocket,self.verbose)
           self.masterCache[mkey] = (db,dbType)
        cu  = db.cursor()
        # send request to DB
        cu.execute(query)
        masterTuple = ()
        if whatToRetrieve=="all":
           masterTuple = cu.fetchall()
        else:
           masterTuple = cu.fetchone()
        return masterTuple
                              
	# Send request to the master DB
#        masterResult = es_init.requestDataFromDB(self.masterDBName,self.masterDBHost,self.masterDBPort,self.masterSocket,query,whatToRetrieve,self.verbose) 
#        if not masterResult:
           # try one more time by contacting global master DB
#           masterResult = es_init.requestDataFromDB("","","","",query,whatToRetrieve,self.verbose)
#        return masterResult

    def queryFileIDTable(self,tup):
        """Retrieve file names and type ids from FileID table.
        @type tup: list
        @param tup: list of file id's
        @rtype: list
        @return: list of triplets (fileId, fileName, typeId).
        """
	oList    = []
        for fid in tup:
	    query= "SELECT fileName,typeid FROM FileID where fileId='%s'"%fid
	    res  = self.requestDataFromDB(query,"one") 
	    file = res[0]
	    typeId = res[1]
	    oList.append((fid,file,typeId))
        return oList

    def findParents(self,iFile):
        """For given input file we find all parents in DB. The fileType of parents should
	be the same as fileType of input file. 
        @type iFile: string
        @param iFile: file name
        @rtype: list
        @return: a tuple (parentDict,presentParents) where
        parentDict[keyTuple]=[keyFileList,locFileList,locDataDict]
        keyTuple is (graphid,run,uniqueId)
        and lists of key and location files as well as
        locDataDict[(keyFileName,locFileName)]=dataFileList
        and presentParents is a flag which indicates if parents were found in DB, regardless of
        input fileType.
        """
	fileType  = file_util.fileType(iFile)
        # get run, uid from iFile
        runList, uidList = file_util.runParser(iFile)
	gidList   = []
        presentParents = 0 # flag which indicates if parents were found in DB, regardless of fileType
	# get graphid of the parent
	for parent in self.parents:
 	    query = """SELECT graphid FROM SpecificVersion,GraphPath WHERE svName='%s' 
	            AND SpecificVersion.svid=GraphPath.svid"""%parent
	    tup   = self.requestDataFromDB(query)
	    for item in tup:
	        if not gidList.count(item[0]):
		   gidList.append(item[0])
        # get all location files for (run,uid)
        parentDict= {}
        graphDict = {}
        for idx in xrange(0,len(runList)):
	    run   = runList[idx]
	    uid   = uidList[idx]
	    for gid in gidList:
               ## not using location files!
               # find out all loc. files of parent
	       #query = "SELECT locationFileId FROM Location WHERE run='%s' AND uid='%s' AND graphid='%s'"%(run,uid,gid)
	       #tup   = self.requestDataFromDB(query)
	       #lidList = []
	       #for item in tup: lidList.append(item[0])
#              # locFileList = self.queryFileIDTable(lidList)
               #locFileList = []
               #for item in self.queryFileIDTable(lidList):
               #    presentParents=1
               #    locFileName = item[1]
               #    if "l"+fileType==file_util.fileType(locFileName):
               #        locFileList.append(item)
	       #if self.verbose:
	       #   print "locFileList",locFileList
	       # find out all non-'all' views from KeyFile
	       query = "SELECT keyFileId,view FROM KeyFile WHERE run='%s' AND uid='%s' AND graphid='%s'"%(run,uid,gid)
	       tup   = self.requestDataFromDB(query)
	       kidList = []
	       viewList= []
	       for item in tup:
	           kidList.append(item[0])
	           viewList.append(item[1])
	       viewKeyList = self.queryFileIDTable(kidList)
	       keyFileList = []
	       keyFileName = ""
	       for idx in xrange(0,len(viewKeyList)):
		   view = viewList[idx]
		   if view=="all": keyFileName=viewKeyList[idx][1]
	           keyFileList.append(viewKeyList[idx]+(view,))
	       if self.verbose:
	          print "keyFileList",keyFileList
	       # find out all parent data files
	       #locDataDict = {}
               #if not len(locFileList): continue
	       #for item in locFileList:
	       #    locFileName = item[1]
	       #    dataFileList= []
	       #    if os.path.exists(locFileName):
	       #       idList=file_util.getFileIds(locFileName)
	       #       dataFileList = self.queryFileIDTable(idList)
	       #       locDataDict[(keyFileName,locFileName)]=dataFileList
               #   # find all other graphs where our location files were presented
               #   locId = item[0]
               #   query = "SELECT graphid FROM Location WHERE locationFileId='%s'"%locId
               #   tup   = self.requestDataFromDB(query)
               #   for elem in tup:
               #       graphDict[elem[0]]=dataFileList
	       keyTuple = (long(gid),int(run),int(uid))
	       parentDict[keyTuple]=[keyFileList,locFileList,locDataDict]
	if self.verbose:
	   print "parentDict",parentDict
           print "graphDict",graphDict
        return graphDict,parentDict,presentParents
        
    def allowStoreToDB(self,iFileList,checkFilesInDB=1):
        """Check if we can add given files to DB, to be allowed they should contain
        non-overlaping data among themselves and with EventStore DB. This method
	also perform data integrity checks on input files. Please consult
	https://wiki.lepp.cornell.edu/CleoSWIG/bin/view/Main/EventStoreAdministration
	for more information.
        @type iFileList: list
        @type checkFilesInDB: integer (default=1)
        @param iFileList: list of files
        @param checkFilesInDB: flag
        @return: a tuple of (oFileList,refFileType,isGroup) where 
        oFileList is output file list, refFileType file type of iFileList,
        isGroup=1 if iFileList can be treated as a group or not, e.g.
        qcd_hot.pds, 2photon_hot.pds, bhaga_hot.pds and unkown_hot.pds should
        be treated as a group, rather pass2.pds, post-pass2.pds and dskim.pds should be treated as
        individual input sources.
        """
        oFileList = []
	
#        if self.verbose:
#           dlog = es_logger.ESLogger('ESManager:allowStoreToDB',('stream',),'debug')
#           dlog.debug("test")
        # check if supplied timeStamp/grade/svName is present in ESDB
	newTimeGradeSVName = 0
        query="""SELECT Version.graphid
		 FROM SpecificVersion,Version,GraphPath
		 WHERE svName='%s' AND grade='%s'
		 AND timeStamp='%s'
		 AND SpecificVersion.svid=GraphPath.svid 
		 AND GraphPath.graphid=Version.graphid
		"""%(self.svName,self.grade,self.timeS)
	tup = self.fetchOne(query)
        if not tup:
	   # no such combination found, therefore we allow to add
	   newTimeGradeSVName = 1
	   graphid = 0
	else:
	   graphid = tup[0]
	   if not self.dictSVName.has_key(self.svName):
	      self.dictSVName[self.svName] = graphid
	   
	# scan input files and identify if any of them are already in DB
	refFileType = ""
	refFileName = ""
	idxaRunUidList  = []
        for file in iFileList:
	    if checkFilesInDB:
               query = "SELECT fileId from FileID WHERE fileName='%s'"%file
	       tup = self.fetchOne(query)
	       if tup:
	          print "The following file is found in EventStore DB\n",file
	          return self.error
	    # check if supplied file format is supported by ES
	    fileType = file_util.fileType(file)
	    if not refFileType:
	       refFileType = fileType
	    if not refFileName:
	       refFileName = file
	       
	    if refFileType!= fileType:
	       print "You are trying to inject different file types"
	       print "We found at least two:",refFileType,fileType
	       return self.error
	    if not self.allowToInject(fileType):
	       print "The following file format is not supported in EventStore"
	       print fileType,file
	       return self.error
	    # for 'idxa' file type collect and check svList
	    if refFileType=="idxa":
	       svList,runUidList = self.readIDXAFile(file)
	       for pair in runUidList:
		   run = pair[0]
		   uid = pair[1]
		   if not self.minRun: self.minRun=run
		   if self.maxRun==1000000: self.maxRun=run
		   if self.minRun>run:
		      self.minRun=run
		   if self.maxRun<run:
		      self.maxRun=run
		   # in the case of idxa files we need to check that supplied view is not
		   # present in ESDB
		   query ="""SELECT fileName from KeyFile,FileID WHERE graphid='%s' AND 
		   view='%s' AND run='%s' AND uid='%s' AND KeyFile.keyFileId=FileID.fileId
		   """%(graphid,self.view,run,uid)
		   tup = self.fetchOne(query)
		   if tup:
		      print "You're trying to create a view='%s' from index files"%self.view
		      for item in iFileList:
			  print item
		      print "But it's already exists and present in"
		      print tup[0]
		      return self.error
	       overlap= self.compareLists(idxaRunUidList,runUidList)
	       if overlap:
		  print "Found overlaping run/uid pairs in supplied list of IDXA files"
		  print "Please check your IDXA files and/or inject one by one"
		  return self.error
	       idxaRunUidList+=runUidList
	       
	# in the case of idxa files pass entire list back for injection
	if refFileType=="idxa":
	   return (iFileList,refFileType,1)
   
	# perform integrity check over input files, checkFileList return two dict's
	# rDict={run: fileList}, dict={file: [runList,pList,svList]}
	output = self.checkFileList(iFileList)
	if output==self.error:
	   return self.error
	rDict   = output[0]
	dict    = output[1]
	isGroup = output[2]
        isGroup = 1   ## HORRIBLE HACK HERE
	
	totalRunList = rDict.keys()
	totalRunList.sort()
	if self.minRun==0 and self.maxRun==1000000:
	   self.minRun=totalRunList[0]
	   self.maxRun=totalRunList[-1]

        # check if supplied timeStamp/grade/svName is present in ESDB
        if newTimeGradeSVName:
	   # no such combination found, therefore we allow to add
	   return (iFileList,refFileType,isGroup)
        
	# We need to look now at the content of the given files and identify
	# if their data are not overlap with ESDB
	
        # check if file from supplied fileList is not present in ESDB
	for f in iFileList:

            # check if run(s) from supplied files are not present in ESDB
	    (runList, pList, svList) = dict[f]
	    uid = svList[0][2]
	    for run in runList:
	        # check if we're adding data with another view
	        query = """SELECT fileName FROM FileID,KeyFile WHERE
		        fileId=keyFileId AND graphid='%s' AND view='%s' 
			AND run='%s' AND uid='%s'
		        """%(graphid,self.view,run,uid)
		keyFileTuple = self.fetchAll(query)
		if not keyFileTuple:
		   if not oFileList.count(f):
                      oFileList.append(f)
		   continue
		# BIG QUESTION: if I found key file which cover
		# graphid,view,run,uid do I need to proceed or is not allowed
		# if I proceed that means key,loc files need to be re-written
		# for the case when data not overlap, otherwise KeyFile table
		# will not allows injection of new key file with the same
		# graphid, view, run, uid
		if keyFileTuple:
		   print "While processing",f
		   print "we found view='%s', run='%s', uid='%s' in:"%(self.view,run,uid)
		   for item in keyFileTuple:
		       print item[0]
		   print "for the following dataVersionName(s)"
		   query="""SELECT svName FROM GraphPath,SpecificVersion WHERE
		            GraphPath.graphid='%s' AND GraphPath.svid=SpecificVersion.svid
	           """%(graphid)
		   for item in self.fetchAll(query):
	               print item[0]
		   print "Abort injection"
		   return self.error
		
                ### NOT USING LOCATION FILES!
                # check if data from supplied files are not present in ESDB
	        # loop over all location files and compare proxies
	        # for given run get proxy list from associative locFile
	        #query = """SELECT fileName FROM Location,FileID 
		#	   WHERE graphid='%s' AND run='%s' and uid='%s'
		#	   AND Location.locationFileId=FileID.fileId
		#	"""%(graphid,run,uid)
	        #nameList = self.fetchAll(query)
	        #for locName in nameList:
		#     locFileName = locName[0]
		#     # loop over all location ids and compare dataTypes (proxies)
		#     # from input file with what was found in locaiton file
		#     type = file_util.fileType(locFileName)
	        #     # locInfo=[streamNames,pdsIDList,proxyDict,recordSize,posOfFirstRecord]
		#     locInfo = file_util.locationFileParser(locFileName)
		#     sNamesInLoc = locInfo[0]
		#     pDictInLoc  = locInfo[2] 
	        #     # form appropriate list [(stream,[listOfProxies])]
	        #     locProxyList=[]
	        #     for str in sNamesInLoc:
		#         locProxyList.append((str,pDictInLoc[str]))
	        #     locProxyList.sort()
		#     lDataTypes=locProxyList
#               #      lDataTypes=locInfo[0] # in old style locationFileParser returns this list
		#     # if list of dataTypes is different allow to add
                #     # compare proxies from supplied files and locationFile
		#     if lDataTypes!=pList:
		#        if not oFileList.count(f):
                #           oFileList.append(f)
		#	continue
		#     else:

                # last step to check if syncValues overlap
                # first find out associative key file
                for item in keyFileTuple:
                    keyFileName = item[0]
                    keySVList=key_dump.keyFileParser(keyFileName)
                    if not keySVList:
                        print "Unable to get key file svList from",keyFileName
                        return self.error
                    # compare svList from supplied file and keyFile
                    svOverlap = self.compareLists(svList,keySVList)
                    if svOverlap:
                        # for now I'm leaving here
                        error ="data overlap for run %s\n"%r
                        error+="You need to specify new grade and/or timeStamp"
                        self.printMsg([fileName],error)
                        return self.error
	    if not oFileList.count(f):
               oFileList.append(f)
        return (oFileList,refFileType,isGroup)
	
    def checkParentsInDB(self):
        """First thing during injection we need to check if provided list of
	parents is already present in DB, otherwise we need to inject parent's
	information.
        @rtype: none
        @return: none
	"""
	for parent in self.parents:
	    if parent=='NULL':
	       continue
	    query  = "SELECT svid FROM SpecificVersion WHERE svName='%s'"%parent
	    tup    = self.fetchOne(query)
            if not tup or not tup[0]:
	       # we need to inject this parent into DB
	       # update GraphPath and SpecificVersion
	       self.startTxn("checkParentsInDB")
	       addToQuery=""
	       if self.dbType=="mysql":
	          addToQuery=" FOR UPDATE"
	       query = "SELECT MAX(svid) FROM SpecificVersion"+addToQuery
	       tup   = self.fetchOne(query)
	       if tup and tup[0]: 
		  svid = int(tup[0])+1
	       else: 
		  svid = 1
	       query = "SELECT MAX(graphid) FROM GraphPath"+addToQuery
	       tup   = self.fetchOne(query)
	       if tup and tup[0]: graphid = tup[0]+1
	       else: graphid = 1
	       query = "INSERT INTO GraphPath (graphid,svid) VALUES('%s','%s')"%(graphid,svid)
	       cQuery="DELETE FROM GraphPath WHERE graphid='%s'"%graphid
	       self.updateDBAndLog(query,cQuery)
	       query = "INSERT INTO SpecificVersion (svName,svid) VALUES('%s','%s')"%(parent,svid)
	       cQuery="DELETE FROM SpecificVersion WHERE svName='%s'"%parent
	       self.updateDBAndLog(query,cQuery)
	       if not self.dictSVName.has_key(parent):
		  self.dictSVName[parent]=graphid
	       self.endTxn("checkParentsInDB")

    def updateVersion(self):
        """Update Version table. The procedure is to check if processed run fall in
	existing run range for given grade/timeStamp. Otherwise form new graphid,
	update GraphPath,SpecificVersion,PathDepend table(s)
        @rtype: none
        @return: if nothing wrong we return graphid which always positive or self.error
	which is always 0.
	"""
        self.writeToLog("updateVersion")

#        if self.verbose:
#           dlog = es_logger.ESLogger('ESManager:updateVersion',('stream',),'debug')
#           dlog.debug("updateVersion")
        if self.minRun==0 and self.maxRun==1000000:
	   print "You need to provide valid run range"
	   return self.error
	# first let's lookup if svid exists for requested svName
	addToQuery=""
	if self.dbType=="mysql":
	   addToQuery=" FOR UPDATE"
	self.startTxn("updateVersion")
        query = """SELECT DISTINCT Version.id,grade,timeStamp,
	minRunNumber,maxRunNumber,Version.graphid,SpecificVersion.svid
	FROM Version,GraphPath,SpecificVersion 
	WHERE GraphPath.graphid=Version.graphid AND GraphPath.svid=SpecificVersion.svid
	AND SpecificVersion.svName='%s' ORDER BY timeStamp DESC"""%(self.svName)
	query+= addToQuery
	tup = self.fetchAll(query)
	if tup and tup[0]:
	   # we found a match for given svName.
	   list = []
	   svidList=[]
	   for item in tup:
	       id        = int(item[0])
	       grade     = item[1]
	       timeStamp = int(item[2])
	       minR      = int(item[3])
	       maxR      = int(item[4])
	       graphid   = int(item[5])
	       svid      = int(item[6])
	       # update dictionary
	       if not self.dictSVName.has_key(self.svName):
		      self.dictSVName[self.svName]=graphid
	       # find min/max run range
	       if minR<self.minRun: self.minRun=minR
	       if maxR>self.maxRun: self.maxRun=maxR
	       # if no timeStamp provided, we append
	       if self.timeS==timeStamp and self.grade==grade:
		  query="""UPDATE Version SET minRunNumber='%s',maxRunNumber='%s'
			   WHERE id='%s'"""%(self.minRun,self.maxRun,id)
	          self.updateDBAndLog(query)
		  self.endTxn("updateVersion")
    		  self.updatePathDepend(svid)
		  return graphid
	       if self.timeS==-1:
	          list.append((self.grade,gen_util.dayAhead(),self.minRun,self.maxRun,graphid))
	       else:
	          list.append((self.grade,self.timeS,self.minRun,self.maxRun,graphid))
    	       svidList.append(svid)
	   # once we scanned all timeStamp we didn't found a match with given one
	   # we'll need to insert a new timeStamp
	   for idx in xrange(0,len(list)):
	       if self.dbType=="sqlite": 
	          query="""INSERT INTO Version 
	                   (id,grade,timeStamp,minRunNumber,maxRunNumber,graphid,state) 
	                   VALUES (NULL,'%s','%s','%s','%s','%s','active') 
	                """%list[idx]
	       else:
	          query="""INSERT INTO Version 
	                   (grade,timeStamp,minRunNumber,maxRunNumber,graphid,state) 
	                   VALUES ('%s','%s','%s','%s','%s','active') 
	                """%list[idx]
	       self.updateDBAndLog(query)
           self.endTxn("updateVersion")
	   for svid in svidList:
    	       self.updatePathDepend(svid)
	   return graphid
	else:
	   # this svName doesn't exists (as its graphid), let's add new entries
	   # into Version, GraphPath, SpecificVersion, PathDepend
	
	   query = "SELECT MAX(svid) FROM SpecificVersion"+addToQuery
	   tup   = self.fetchOne(query)
	   if tup and tup[0]: 
	      svid = int(tup[0])+1
	   else: 
	      svid = 1
	   query = "SELECT MAX(graphid) FROM GraphPath"+addToQuery
	   tup   = self.fetchOne(query)
	   if tup and tup[0]: graphid = tup[0]+1
	   else: graphid = 1
	   query = "INSERT INTO GraphPath (graphid,svid) VALUES('%s','%s')"%(graphid,svid)
	   cQuery="DELETE FROM GraphPath WHERE graphid='%s'"%graphid
	   self.updateDBAndLog(query,cQuery)
	   query = "INSERT INTO SpecificVersion (svName,svid) VALUES('%s','%s')"%(self.svName,svid)
	   cQuery="DELETE FROM SpecificVersion WHERE svName='%s'"%self.svName
	   self.updateDBAndLog(query,cQuery)
	   if not self.dictSVName.has_key(self.svName):
	      self.dictSVName[self.svName]=graphid
	      
	   # in the case of SQLite auto_increment is working when NULL has been put
	   # into autoincrment column
	   timeStamp = self.timeS
	   if self.timeS==-1:
	      timeStamp = gen_util.dayAhead()
	   if self.dbType=="sqlite": 
	      query="""INSERT INTO Version 
	        (id,grade,timeStamp,minRunNumber,maxRunNumber,graphid,state) 
	        VALUES (NULL,'%s','%s','%s','%s','%s','active') 
	      """%(self.grade,timeStamp,self.minRun,self.maxRun,graphid)
	   else:
	      query="""INSERT INTO Version 
	        (grade,timeStamp,minRunNumber,maxRunNumber,graphid,state) 
	        VALUES ('%s','%s','%s','%s','%s','active') 
	      """%(self.grade,timeStamp,self.minRun,self.maxRun,graphid)
	   cQuery="""DELETE FROM Version WHERE grade='%s'
		     AND timeStamp='%s' AND minRunNumber='%s'
		     AND maxRunNumber='%s' AND graphid='%s'"""%(self.grade,timeStamp,self.minRun,self.maxRun,graphid)
	   self.updateDBAndLog(query,cQuery)
	self.endTxn("updateVersion")
	self.updatePathDepend(svid)
	return graphid
	
    def updatePathDepend(self,svid):	
        """Update PathDepend table.
        @type svid: integer
        @param svid: specific version id
        @rtype: none
        @return: none
        """
        self.writeToLog("updatePathDepend")
	self.startTxn("updatePathDepend")
	# check if parent is NULL
	if (len(self.parents)==1 and self.parents[0].upper()=='NULL') or not self.parents:
	   query="SELECT parentId FROM PathDepend WHERE childId='%s' AND parentId='NULL'"%svid
	   tup  = self.fetchOne(query)
	   if tup:
              # we already have this entry in DB
	      self.endTxn("updatePathDepend")
	      return self.ok	   
	   query="INSERT INTO PathDepend (parentid,childid) VALUES('NULL','%s')"%svid
	   self.updateDBAndLog(query)
	   self.endTxn("updatePathDepend")
	   return self.ok
	# finally we need to update PathDepend table
	parentIds = ()
	if len(self.parents):
	   query = "SELECT svid FROM SpecificVersion WHERE svName='%s'"%(self.parents[0])
	   for idx in xrange(1,len(self.parents)):
	       svName = self.parents[idx]
	       query += "OR svName='%s'"%svName
	   parentIds = self.fetchAll(query)
	for id in parentIds:
	   parentId = id[0]
	   if parentId==svid:
	      if self.verbose:
	         print "WARNING: found request to update PathDepend with parentId('%s')==childId('%s'), will skip it"%(parentId,svid)
		 continue
	   query = "SELECT parentid,childid FROM PathDepend WHERE parentid='%s' AND childid='%s'"%(parentId,svid)
	   tup = self.fetchOne(query)
	   if not tup:
	      query="INSERT INTO PathDepend (parentid,childid) VALUES('%s','%s')"%(parentId,svid)
	      cQuery="DELETE FROM PathDepend WHERE parentid='%s' AND childid='%s'"%(parentId,svid)
	      self.updateDBAndLog(query,cQuery)

	self.endTxn("updatePathDepend")
        return self.ok
	
    def getRunUidListFromFiles(self,fileList):
        """Scan all files and return runUidList from them.
        @type fileList: list
        @param fileList: list of files
        @rtype: list
        @return: list of run and uids presented in input file list
        """
	runUidList= []
	if file_util.fileType(fileList[0])=="idxa":
	   svList,runUidList = self.readIDXAFile(fileList[0])
	else:
	   for f in fileList:
	       if self.cacheFileContent.has_key(f):
		  content = self.cacheFileContent[f]
	       else:
		  content = file_util.fileParser(f)
		  self.cacheFileContent[f]=content
	       runList = content[0]
	       uidList = content[1]
	       for idx in xrange(0,len(runList)):
		   pair= (runList[idx],uidList[idx])
		   if not runUidList.count(pair):
		      runUidList.append(pair)
	runUidList.sort()
	return runUidList
	
#    def getFileInfo(self,iFile):
#        """Form a lists of runs, syncValues and proxies from given file.
#        @type iFile: string
#        @param iFile: file name
#        @rtype: tuple
#        @return: (runList,svList,proxyList), list of runs, sv's and proxies presented in a file
#        """
#	runList   = []
#	svList    = []
#	proxyList = []
#	if file_util.fileType(iFile)=="idxa":
#	   svList,runUidList = self.readIDXAFile(iFile)
#	   for pair in runUidList:
#	       runList.append(pair[0])
#	else:
#	   if self.cacheFileContent.has_key(f):
#	      content = self.cacheFileContent[f]
#	   else:
#	      content = file_util.fileParser(f)
#	      self.cacheFileContent[f]=content
#	   runList    = content[0]
#	   svList     = content[3]
#	   proxyList  = content[2]
#	return (runList,svList,proxyList)
	
    def checkFileList(self,fileList):
        """Check if run/data from provided fileList are unique
        @type fileList: list
        @param fileList: list of files
        @rtype: tuple
        @return: (rDict,dict,isGroup), two dictionaries: (run: fileList), 
        (file: [runList,svList])
        svList list of sync Values presented in a file
        and isGroup - a flag which tell how to treat input file list.
        """

	if file_util.fileType(fileList[0])=="idxa":
	   return ["idxa"]
        dict  = {}
	rDict = {}
	dictStream = {}
	dictProxies= {}
	dictFiles  = {}
	isGroup    = 1   # flag which decide can we treat all source as a group for injection
	runList    = []  # unique list of runs collected from all sources 
        for f in fileList:
	    #if self.cacheFileContent.has_key(f):
	    #   content = self.cacheFileContent[f]
	    #else:
            content = file_util.fileParser(f)
	    #   self.cacheFileContent[f]=content
	       
            #if self.verbose:
            #    print "output of file parser: ",content

	    # if the runList of the parsed file is different from already seen one
	    # we cannot treat all sources as a group since they have different list of runs
	    if runList and runList!=content[0]:
	       isGroup=0
	       
	    runList = content[0]
	    svList  = content[2]
	    dictFiles[f]= content[2]
	    dict[f]=[runList,svList]
			    
            if self.verbose:
                print "for file ",f
                print "the run list is: ",runList

	    # form dictionary {run:[fileList, svList],...}
	    fList   = []
	    for r in runList:
		if rDict.has_key(r): fList=rDict[r]
		if not fList.count((f,svList)):
		       fList.append((f,svList))
		rDict[r]=fList
		fList = []
#        print "dictStream",dictStream
#        print "dictProxies",dictProxies
#        print "dictFiles",dictFiles
	# form a new list of zippedProxies: i.e. we keep one proxy who has the same
	# number of files as others

        if self.verbose:
	   print "We need to analyze",fileList
        if len(fileList)==1:
            if self.verbose:
                print "Only one file supply data, skip analyzer"
            # we can skip the rest
            return (rDict,dict,isGroup)
        if self.verbose:
            #print "\nAnalyzing data in '%s' stream"%stream
            #name,usage,prod=string.split(proxyName,"_tag_")
            #print "['%s','%s','%s']"%(name,usage,prod)
            for f in fileList:
                print f
            print

	return (rDict,dict,isGroup)
                
        idxList=[0]*len(fileList)
        fileIdx=0
        counter=0
        usedFile=""
        while 1:
            tmpList=[]
            tmpFileList=[]
            smallestSVidx=0
            smallestSV=""
            for idx in xrange(0,len(fileList)):
                file=fileList[idx]
                try:
                    fileSV=dictFiles[file][idxList[idx]]
                except:
                    continue
#                while fileSV[-1]!=stream:
#                    idxList[idx]+=1
#                    if len(dictFiles[file])==idxList[idx]:
#                        break
#                    try:
#                        fileSV=dictFiles[file][idxList[idx]]
#                    except:
#                        print "Exception thrown on",file,fileSV,stream
#                        print fileList
#                        print idxList
#                        raise
                tmpList.append(fileSV)
                tmpFileList.append(file)
                if not smallestSV:
                    smallestSV=fileSV
                    smallestSVidx=idx
                    usedFile=file
                else:
                    # check if two SV's have the same stream,run and then compare events
                    # fileSV=(run,event,uid)
                    #print "smallestSV = %s   fileSV = %s" % (str(smallestSV),str(fileSV))
                    #if fileSV[3]==smallestSV[3] and fileSV[0]==smallestSV[0] and fileSV[1]<=smallestSV[1]:
                    if fileSV[0]==smallestSV[0] and fileSV[1]<=smallestSV[1]:
                        smallestSV=fileSV
                        smallestSVidx=idx
                        usedFile=file
                idxList[smallestSVidx]+=1
                if self.verbose and smallestSV:
                    print smallestSV[:-1],usedFile
                # if we reach EOF of all files tmpList should be empty, time to quit the loop
                if not tmpList:
                    break
                # if we find two duplicates, fire up
                for item in tmpList:
                    if tmpList.count(item)>1:
                        recordName,usageTag,prodTag=string.split(proxyName,"_tag_")
                        msg="['%s','%s','%s'], for (%s,%s,%s) in '%s' stream"%(recordName,usageTag,prodTag,item[0],item[1],item[2],item[3])
                        print "--------------------------"
                        if self.dupRead:
                            if self.verbose:
                                print "WARNING: we found data duplication"
                                print msg
                                print "all duplicate data will be taken from",self.dupRead
                                print "--------------------------"
                            return (rDict,dict,isGroup)
                        else:
                            print "ERROR: we found data duplication"
                            print msg
                            print "Please investigate the following list of files:"
                            for tmpFile in tmpFileList:
                                print tmpFile
                            print 
                            print "OR supply -dupRead <fileName> option which will be used to resolve duplication"
                            return self.error
	return (rDict,dict,isGroup)
	
    def getVersionInfo(self,iFileList):
        """Parse input file list and try to get version information from 'beginrun'
	stream according to specifications.  Need to decide these
	The real job is done by L{pds_dump.decodeVersionInfo} method.
        @type iFileList: list
        @param iFileList: list of files
        @rtype: integer
        @return: status code
	"""
        fileSVName = fileParents = ""
        for file in iFileList:
	    if file_util.fileType(file)=="pds":
	       svName,parents,verList,verDict = pds_dump.decodeVersionInfo(file)
	       if not fileSVName: fileSVName=svName
	       if not fileParents: fileParents=parents
	       if fileSVName!=svName:
	          print "While retrieving VersionInfo we found different svNames"
		  print "'%s' svName found in %s"%(fileSVName,iFileList[0])
		  print "'%s' list found in %s"%(svName,file)
		  return self.error
	       if fileParents!=parents:
	          print "While retrieving VersionInfo we found different parent lists:"
		  print "'%s' list found in %s"%(fileParents,iFileList[0])
		  print "'%s' list found in %s"%(parents,file)
		  return self.error
	if fileSVName:
	   self.setSVName(fileSVName)
	   if fileParents:
              self.setParents(parents)
	   else:
	      self.setParents(['NULL'])
	return self.ok
	
    def checkVersionInfo(self,iFileList):
        """Verify that versioning information either setup throguh command line interface
	or read from input file list.
        @type iFileList: list
        @param iFileList: list of files
        @rtype: integer
        @return: status code
        """
        status = self.getVersionInfo(iFileList)
	if status==self.error:
	   print "Error occured during version info scan"
	   return self.error
        if not self.svName:
	   print "No dataVersionName found in supplied files or command line"
	   return self.error
	if file_util.fileType(iFileList[0])!="idxa":
           if not len(self.parents):
	      print "No parent list found in supplied files or command line"
	      return self.error
	return self.ok
	   
    def updateDB(self, genMode, iFileList, oHSMDir=""):
        """Main method to update EventStore DB. Based on provided file list it decide
	how to inject data into EventStore. Database is open by using L{openDBs}, all
        input files are checked by L{allowStoreToDB},
        then we update Version table using L{updateVersion}. Finally, based on file type
        of input files we either use
        L{updateDBFromIDXA}, L{updateDBUsingFileList} or L{updateDBUsingGroupList}
        to do actual job.
        @type genMode: integer
        @type iFileList: list
        @type oHSMDir: string
        @param genMode: flag to generate files
        @param iFileList: list of files
        @param oHSMDir: optional location of HSM directory where files copies will go
        @rtype: integer
        @return: status code
        """
        self.writeToLog("updateDB")
	
        # initialize EventStore databases
        DB = self.openDBs()
	
	# get version information
	if self.checkVersionInfo(iFileList)==self.error:
	   return self.error

        # check if underlying DB is the same as master DB
        currentMasterString = "%s@%s:%s:%s"%(self.masterDBName,self.masterDBHost,self.masterDBPort,self.masterSocket)
        currentDBString = "%s@%s:%s:%s"%(self.dbName,self.dbHost,self.dbPort,self.dbSocket)
        if currentDBString!=currentMasterString:
           # setup maxId by consulting the master DB
           maxId = self.getMaxId()
           query="SELECT fileId FROM FileID WHERE fileId='%s'"%maxId
           tup  = self.fetchOne(query)
           if not tup:
              self.updateFileID(maxId,"",0)

	# perform various tests and decide do we allow injection
        output   = self.allowStoreToDB(iFileList)
	if output==self.error:
	   return self.error
	try:
	   fList    = output[0]
	   if self.dupRead:
	      fList.remove(self.dupRead)
	      fList.insert(0,self.dupRead)
	   fileType = output[1]
	   isGroup  = output[2]
	except:
	   print "output from allowStoreToDB",output
	   raise
       	
	# first we need to check if information about parents already exists in DB,
	# otherwise we need update DB with parent's info
	self.checkParentsInDB()

	# update Version if necessary
	status = self.updateVersion()
        if status==self.error:
	   return self.error
	   
	# inject idxa files
	if fileType=="idxa":
	   for file in fList:
	       # if asked to add idxa file
	       status=self.updateDBFromIDXA(file)
	       if status==self.error:
		  print "While processing IDXA file %s"%(file)
		  return self.error
	   return status
		  
	# start injection
	if isGroup and ( fileType=='hddm' or fileType=='evio' or fileType=='rest' or fileType=='mc' ) : 
	   # inject fList as a group
	   status=self.updateDBUsingGroupList(fList,oHSMDir)
	else:
	   # normal case of creating/updating DB based on fileList
	   status=self.updateDBUsingFileList(genMode,fList,oHSMDir)
	
	# print content of all DBs
        if self.verbose: 
	   print "Update DB status:",status 
        return status

    def uniqueList(self,iList):
        """Eliminates duplicates from provided list and return back unique list"""
	oList = []
	for elem in iList:
	    if not oList.count(elem):
	       oList.append(elem)
	return oList

    def decodeKeyFile(self,keyFileName):
        """Decode content of key file. It just invokes L{key_dump.dump} method.
        @type keyFileName: string
        @param keyFileName: file name
        @rtype: list
        @return: a list of (run,evt,uid,stream)
        """
        return key_dump.dump(keyFileName,0,"null")
	
    def getGraphIds(self,all=""):
	"""Return a list of parent graph id's. Can perform nested lookup if all
        parameter is specified.
        @type all: integer or string
        @param all: optional parameter to check parents by using L{getAllParents}
        @rtype: list
        @return list of graph id's
        """
	graphIdList= []
	for name in self.parents:
	    query  = """SELECT Version.graphid FROM Version,SpecificVersion,GraphPath 
	    WHERE Version.graphid=GraphPath.graphid AND SpecificVersion.svName='%s'
	    AND GraphPath.svid=SpecificVersion.svid
	    """%name
	    tup=self.fetchAll(query)
	    for item in tup:
	        if not graphIdList.count(item[0]):
                   graphIdList.append(item[0])
	    if  all:
	        # need to collect all parents
	        dList,idList,dict,dictId,graph = self.getAllParents(name)
#                print "For child",name,"get back",dList,idList
   		for id in idList:
		    if not graphIdList.count(id):
		       graphIdList.append(id)
	return graphIdList
   
    def getLocAndKeyFromParent(self,fileList):
        """Get list of parent pairs (id,fileName) for key/loc. files from
	provided list of files. 
	  - key file case: 
	      - we only lookup immediate parents
	  - loc file case:
	      - we lookup all parents in dependency tree
        @type fileList: list
        @param fileList: list of files
        @rtype: tuple
        @return: a tuple of three lists: (locFileList,keyFileList,viewFileList)
        where viewFileList is a file list of key files with non-all views.
	"""
        self.writeToLog("getLocAndKeyFromParent")
        if self.verbose: print "Using getLocAndKeyFromParent"

	# get a list of key files from parent grade which cover runs from fileList
	runUidList = self.getRunUidListFromFiles(fileList)
	graphIdList= self.getGraphIds()
	keyFileList=[]
	viewFileList=[]
	for graphid in graphIdList:
	    for item in runUidList:
	        run = item[0]
		uid = item[1]
		query ="""SELECT fileId,fileName FROM KeyFile,FileID WHERE run='%s'
		AND uid='%s' AND KeyFile.keyFileId=FileID.fileId AND graphid='%s' 
		AND view='all'"""%(run,uid,graphid)
		tup=self.fetchAll(query)
		for item in tup:
		    pair = (item[0],item[1]) # (fileId,fileName)
		    if not keyFileList.count(pair):
		       keyFileList.append(pair)
		query ="""SELECT fileId,fileName,view FROM KeyFile,FileID WHERE run='%s'
		AND uid='%s' AND KeyFile.keyFileId=FileID.fileId AND graphid='%s' 
		AND view!='all'"""%(run,uid,graphid)
		tup=self.fetchAll(query)
		for item in tup:
		    triplet = (item[0],item[1],item[2]) # (fileId,fileName,view)
		    if not viewFileList.count(triplet):
		       viewFileList.append(triplet)
	locFileList=[]
        ### not using location files!
	#graphIdList= self.getGraphIds("forAllParents")
	#for graphid in graphIdList:
	#    for item in runUidList:
	#        run = item[0]
	#	uid = item[1]
	#	query ="""SELECT fileId,fileName FROM Location,FileID WHERE run='%s'
	#	AND uid='%s' AND Location.locationFileId=FileID.fileId AND graphid='%s' 
	#	"""%(run,uid,graphid)
	#	tup=self.fetchAll(query)
	#	for item in tup:
	#	    pair = (item[0],item[1]) # (fileId,fileName)
	#	    if not locFileList.count(pair):
	#	       locFileList.append(pair)
	if self.verbose:
           print "loc-files of the parents:"
	   for file in locFileList:
	       print file
           print "key-files of the parents:"
	   for file in keyFileList:
	       print file
	   for file in viewFileList:
	       print file
	return (locFileList,keyFileList,viewFileList)

    def updateDBUsingGroupList(self,fileList,oHSMDir=""):
        """Inject provided files into EventStore as a logical group, 
	e.g. qcd_hot_runX, 2photon_hot_runX, etc. Key and location files are
        generated by using L{generateLocAndKeyFilesFrom}. All tables in DB are
        updated by using: L{updateKeyFile}, L{updateRunUID} and L{updateLocation}.
        @type fileList: list
        @param fileList: list of files
        @type oHSMDir: string
        @param oHSMDir: optional HSM location where files copies will go
        @rtype: integer
        @return: status code
        """
        self.writeToLog("updateDBUsingGroupList")
        if self.verbose: print "Using updateDBUsingGroupList"

        # copy files to HSM if necessary
        if oHSMDir:
           for file in fileList:
               shutil.copy(file,oHSMDir)
	# generate loc and key files from given fileList
	result = self.generateLocAndKeyFilesFrom(fileList)
	if result==self.error:
	   return self.error
        oFileDict = result[0]
        oKeyDict  = result[1]
        oLocDict  = result[2]
#        print "oFileDict",oFileDict
#        print "oKeyDict",oKeyDict
#        print "oLocDict",oLocDict
	graphid   = self.dictSVName[self.svName]
        
        # update FileID table
        for fileId in oFileDict:
            fList = oFileDict[fileId]
            for item in fList:
                fileName = item
                typeId   = self.updateFileType(fileName)
                self.updateFileID(fileId,fileName,typeId)
        
        # update KeyFile table
        for key in oKeyDict:
            run = key[0]
            uid = key[1]
            view= key[2]
            kid,name = oKeyDict[key]
	    self.updateRunUID(run,uid)
	    query="""SELECT keyFileId FROM KeyFile
		     WHERE graphid='%s' AND view='%s' AND run='%s' AND uid='%s'
		  """%(graphid,view,run,uid)
	    data= self.fetchOne(query)
            if not data:
	       self.updateKeyFile(graphid,view,run,uid,kid)
            
        ### not using locations!
        # update Location table
        #for key in oLocDict:
        #    run = key[0]
        #    uid = key[1]
        #    lidList = oLocDict[key]
        #    for lid in lidList:
	#        self.updateLocation(graphid,run,uid,lid)
	# commit transaction
        return self.ok
        
    def updateDBUsingFileList(self, generationMode, fileList, oHSMDir=""):
        """Inject sequentially files into EventStore. This method is used
        to inject raw data files to ESDB.
        Key and location files are
        generated by using 
        L{build_binary_key_loc.build_binKeyAndLoc} in case of binary input files or
        L{genFile}. All tables in DB are
        updated by using: L{updateKeyFile}, L{updateRunUID} and L{updateLocation}.
        @type generationMode: integer
        @type fileList: list
        @type oHSMDir: string
        @param generationMode: flag to generate key/loc. files
        @param fileList: list of files
        @param oHSMDir: optional file name of binary file
        @rtype: integer
        @return: status code
        """
        self.writeToLog("updateDBUsingFileList")

        if self.verbose: print "Using updateDBUsingFileList"
	print "\nProcessing files:"

    	for fileName in fileList:

	   print fileName

	   # get run/uid lists directly from hddm file
	   if self.cacheFileContent.has_key(fileName):
	      content = self.cacheFileContent[fileName]
	   else:
	      content = file_util.fileParser(fileName)
	      self.cacheFileContent[fileName]=content
	   runList = content[0]
	   uidList = content[1]

    	   # notations
	   #idList = self.getIds(3)
    	   #dat_id = idList[0]; key_id = idList[1]; loc_id = idList[2]
	   idList = self.getIds(2)
    	   dat_id = idList[0]; key_id = idList[1];

    	   # generate key and location files
           ### except not using locations for now
    	   datId   = "%d"%dat_id
    	   if generationMode: 
	      #if file_util.fileType(fileName)=="bin":
	      #   # use new module to build key/loc. files together
	      #   keyFile = self.genFileName(fileName,key_id,"key")
	      #   #locFile = self.genFileName(fileName,loc_id,"location")
              #   if oHSMDir:
              #      binFile = os.path.join(oHSMDir,os.path.split(fileName)[1])
              #   else:
              #      binFile = ""
              #   if self.verbose:
              #      print "Copy '%s' to HSM '%s'"%(fileName,binFile)
	      #   build_binary_key_loc.build_binKeyAndLoc(fileName,datId,locFile,keyFile,binFile)
	      #else:
               keyFile = self.genFile(fileName,key_id)
               #print "genMode %s %s"%(generationMode,keyFile)
               #locFile = self.genFile(fileName,loc_id,datId)
	   else:
	      keyFile = self.genFileName(fileName,key_id,"key")
              #print "noGenMode %s"%keyFile
	      #locFile = self.genFileName(fileName,loc_id,"location")

    	   # update entries in FileID
    	   datFile = fileName
	   datType = self.updateFileType(datFile)
	   self.updateFileID(dat_id,datFile,datType)
	   keyType = self.updateFileType(keyFile)
	   self.updateFileID(key_id,keyFile,keyType)
	   #locType = self.updateFileType(locFile)
	   #self.updateFileID(loc_id,locFile,locType)

	   # loop over run/uid list
	   i = 0
	   graphid = self.dictSVName[self.svName]
	   while i<len(runList):
	      run = int(runList[i])
	      uid = int(uidList[i])
	      i   = i+1

	      # update entries in KeyFile
	      query="""SELECT fileName FROM FileID,KeyFile
		       WHERE graphid='%s' AND view='%s' AND run='%s' AND uid='%s'
		       AND FileID.fileId=KeyFile.keyFileId
		    """%(graphid,self.view,run,uid)
	      data = self.fetchOne(query)
	      if data: 
	         keyFileName = data[0]
	         msg = "Found key file '%s' which cover (run,uid)=(%s,%s) from %s"%(keyFileName,run,uid,fileName)
		 self.printMsg([fileName],msg)
	         return self.error # error
	      self.updateKeyFile(graphid,self.view,run,uid,key_id)

	      # update entries in Location
              #self.updateLocation(graphid,run,uid,loc_id)

	      # update properly RunUID
	      self.updateRunUID(run,uid)
	   
	return self.ok

    def readIDXAFile(self,fileName):
        """Read ASCII idxa file and return syncValue and runUid lists.
        @type fileName: string
        @param fileName: name of the file
        @rtype: tuple
        @return: a tuple of two lists: svList and runUidList
        """
        self.writeToLog("readIDXAFile")
        if self.idxaDict.has_key(fileName):
           reader = self.idxaDict[fileName]
        else:
           reader = idxa_reader.IDXAFileReader(fileName)
           self.idxaDict[fileName]=reader
        return (reader.getSVList(),reader.getRunUidList())
        # read file and create syncValue list to parse
#        lines = open(fileName,'r').readlines()
#        svList= []
#        runUidList = []
#        startToRead = 0
#        for x in lines:
#            if string.find(x,"IDXD")!=-1:
#               startToRead=1
#               continue # we will read from next line
#            if startToRead:
#               sf = string.split(string.replace(x,"\n","")," ")
#               svList.append( (int(sf[1]),int(sf[2]),int(sf[3]) ) )
#               runUidPair = (int(sf[1]),int(sf[3]))
#               if not runUidList.count(runUidPair):
#                  runUidList.append(runUidPair)
#        return (svList,runUidList)
	
    def updateDBFromIDXA(self,fileName):
        """Inject information from idxa file into EventStore. In this case we
	only create a new index (key) file.
        @type fileName: string
        @param fileName: name of the file
        @rtype: integer
        @return: status code
        """
        self.writeToLog("updateDBFromIDXA")

        if self.verbose: print "Using updateDBFromIDXA"
	
	if self.view=="all":
	   msg="You requested to add selection of events.\n"
	   msg+="Please assign new 'view' for them through -view option"
	   self.printMsg([fileName],msg)
   	   return self.error # error 
   
	id = self.getIds(1)

	# read IDXA file and find index for the first run
        idxaFile = open(fileName,'r')
	lines = idxaFile.readlines()
        idxaFile.close()
	startToRead = 1
	#for idx in xrange(0,len(lines)):
	#    x = lines[idx]
	#    if string.find(x,"IDXD")!=-1:
	#       startToRead=idx+1
	#       break

	nSyncValues= len(lines[startToRead:])
	currentRun = 0
	needToSwap = 0
	outputList = []
	outputFile = ""
	keyFile    = ""
	currentKeyFileName=""
	processedSV = (0,0,0)
	print "===> Processing"

	#totalString = string.join(lines[startToRead:])
	counter=0

        ## For multithreaded jobs, the order of processed events is not preserved
        ## therefore, skimmed index files may not have events in the same order as 
        ## in the files.  To deal with this, we read in the entire IDXA file and
        ## build lists of sync values, indexed by (run,uid).  Then, we go through
        ## the key files corresponding to the (run,uid) pairs, and only output
        ## the syncvalues contained in the index files
        syncValueDict = {}
        graphid = self.dictSVName[self.svName]
        # read in the IDXA file!
	for index in xrange(startToRead,len(lines)):
	    sf = string.split(string.replace(lines[index],"\n","")," ")
	    sv = ( int(sf[0]),int(sf[1]),int(sf[2]) )
#            print "sv",sv,processedSV
#	    if sv==processedSV: continue
	    run = int(sv[0])
	    uid = int(sv[2])

            if not (run,uid) in syncValueDict:
                syncValueDict[(run,uid)] = []
            syncValueDict[(run,uid)].append(sv)

        # now go through the key files and build new ones for each index file
        for run,uid in syncValueDict.keys():
	    listOfSyncValues = syncValueDict[(run,uid)]  ## sync values from skim to process
            #print " processing index for ",run,uid
            #print " syncValues:"
            #print str(listOfSyncValues)

            # increment fileID every time when run is changed
            if currentRun: id=self.getIds(1)
            # form output file name
            fName = fileName+"-run%d"%run
#           print "fName",fName,id
#           outputFileName = self.genFileName(fName,id,"key")
            outputFileName = self.genFileName(fileName,id,"key")
            print outputFileName

            #outputFile.close()  ####  CLOSE FILES
            outputFile = open(outputFileName,'w+b')
	       
            # keep track of opened run/outputFileNames for later store to DB
            outputList.append( (outputFileName,id,run,uid,graphid,outputFile) )
	
            # loookup in DB and open key file for that run
            query="""SELECT fileName FROM FileID,KeyFile
			WHERE graphid='%s' AND view='all' AND run='%s' AND uid='%s'
			AND KeyFile.keyFileId=FileID.fileId
		     """%(graphid,run,uid)
            data = self.fetchOne(query)
            if data:
                keyFileName = data[0]
            else:
                error = "No associative key file found for run %d"%run
                self.printMsg([],error)
                return self.error # error

            # now read in the key file!
            keyFileHeader = key_dump.keyFileHeaderReader(keyFileName)
            if not keyFileHeader: 
                msg="Problem with reading key file header %s"%keyFileName
                self.printMsg([],msg)
                print "keyFileHeader",keyFileHeader
                return self.error # error
            needToSwap     = keyFileHeader[1]
            numKeyRecords  = keyFileHeader[3]

            keyFile= open(keyFileName,'r')
            # create header and write it to output file
            header = array.array('I')
            header.fromfile(keyFile, 3)
            if needToSwap:
                header.byteswap()
            header[2] = len(listOfSyncValues)
            header.tofile(outputFile)
            #sys.__stdout__.flush()
  
   	    # read syncValues from keyFile and write those which match into
   	    # output file
            #print "number of records to run over = " + str(numKeyRecords)
            for keyInd in xrange(numKeyRecords):
	       try:
	          record = array.array('I')
	          record.fromfile(keyFile,6)
		  if needToSwap: record.byteswap()
	          syncValue = (int(record[0]),int(record[1]),int(record[2]))
                  #print "syncValue",syncValue
   		  if syncValue in listOfSyncValues:    ## this is really slow!! try to speed it up:
		     record.tofile(outputFile)
                     counter+=1
		     #break
   	       except EOFError:
	          break

            outputFile.close()
            keyFile.close()
	    ################# Fix key file header with correct number of SV's
	    # seek to position where total SV's written
            #outputFile.seek(0)
            #nWordsArray = array.array('I')
            #nWordsArray.fromfile(outputFile,2)
            #pos = outputFile.tell()
            #nSVHeader = array.array('I')
            #nSVHeader.fromfile(outputFile,1)
            #outputFile.seek(pos)
            #nSVHeader[0]=counter
            #nSVHeader.tofile(outputFile)
	    #################
	    sys.__stdout__.flush()


	# put generated key files into DB
	for x in outputList:
	    name   = x[0]
	    fileID = x[1]
	    run    = x[2]
	    uid    = x[3]
	    graphid   = x[4]
	    oFile  = x[5]

	    # first we need to close all opened files
	    oFile.close() 
	    # update FileID
	    typeId = self.updateFileType(name)
	    self.updateFileID(fileID,name,typeId)

	    # update KeyFile
            self.updateKeyFile(graphid,self.view,run,uid,fileID)

	# change permission
	os.chmod(outputFileName,0444)

	return self.ok

    def updateDBFromIDXA_OLD(self,fileName):
        """Inject information from idxa file into EventStore. In this case we
	only create a new index (key) file.
        @type fileName: string
        @param fileName: name of the file
        @rtype: integer
        @return: status code
        """
        self.writeToLog("updateDBFromIDXA")

        if self.verbose: print "Using updateDBFromIDXA"
	
	if self.view=="all":
	   msg="You requested to add selection of events.\n"
	   msg+="Please assign new 'view' for them through -view option"
	   self.printMsg([fileName],msg)
   	   return self.error # error 
   
	id = self.getIds(1)

	# read IDXA file and find index for the first run
	lines = open(fileName,'r').readlines()
	startToRead = 1
	#for idx in xrange(0,len(lines)):
	#    x = lines[idx]
	#    if string.find(x,"IDXD")!=-1:
	#       startToRead=idx+1
	#       break

	nSyncValues= len(lines[startToRead:])
	currentRun = 0
	needToSwap = 0
	outputList = []
	outputFile = ""
	keyFile    = ""
	currentKeyFileName=""
	processedSV = (0,0,0)
	print "===> Processing"

	totalString = string.join(lines[startToRead:])
	counter=0
	# loop over all entries in input file, form syncValue and do a job
        graphid = self.dictSVName[self.svName]
	for index in xrange(startToRead,len(lines)):
	    sf = string.split(string.replace(lines[index],"\n","")," ")
	    sv = ( int(sf[0]),int(sf[1]),int(sf[2]) )
#            print "sv",sv,processedSV
	    if sv==processedSV: continue
	    run = int(sv[0])
	    uid = int(sv[2])

	    if currentRun!=run:

	       # increment fileID every time when run is changed
	       if currentRun: id=self.getIds(1)
	       # form output file name
	       fName = fileName+"-run%d"%run
#               print "fName",fName,id
#               outputFileName = self.genFileName(fName,id,"key")
	       outputFileName = self.genFileName(fileName,id,"key")
	       print outputFileName
	       if outputFile:
	          outputFile.close()
#               outputFile = open(outputFileName,'wb')
	       outputFile = open(outputFileName,'w+b')
	       
	       # keep track of opened run/outputFileNames for later store to DB
	       outputList.append( (outputFileName,id,run,sv[2],graphid,outputFile) )
	
	       # loookup in DB and open key file for that run
               query="""SELECT fileName FROM FileID,KeyFile
			WHERE graphid='%s' AND view='all' AND run='%s' AND uid='%s'
			AND KeyFile.keyFileId=FileID.fileId
		     """%(graphid,run,uid)
	       data = self.fetchOne(query)
	       if data:
	          keyFileName = data[0]
	       else:
	          error = "No associative key file found for run %d"%run
		  self.printMsg([],error)
   		  return self.error # error
	       # we need to open only new key file, while it's done close old one
	       if currentKeyFileName!=keyFileName:
		  keyFileHeader = key_dump.keyFileHeaderReader(keyFileName)
		  if not keyFileHeader: 
		     msg="Problem with reading key file header %s"%keyFileName
		     self.printMsg([],msg)
		     print "keyFileHeader",keyFileHeader
		     return self.error # error
		  needToSwap    = keyFileHeader[1]
		  nRecordTypes  = keyFileHeader[2]

#               if currentKeyFileName!=keyFileName:
	          # close previous keyFile if it was opened
		  if keyFile:
		     keyFile.close()
		  keyFile= open(keyFileName,'r')
	          # create header and write it to output file
	          header = array.array('I')
		  header.fromfile(keyFile, 3)
		  if needToSwap:
		     header.byteswap()
                  #nWords = header[2]
		  #nNameWords = header[-1]
		  header.tofile(outputFile)
	

	          # count how many times given run present in a list
	          # substruct number of 0's event duplicates
	          # add 1 to account for event number 0.
	          nSyncValues = totalString.count(str(run))
                  nSyncValues = nSyncValues-totalString.count( "%s 0"%run )+1
   		  # write the rest of the header to output file
		  #restHeader = array.array('I')
		  #restHeader.fromlist([nSyncValues,nWords])
	          #restHeader.tofile(outputFile)
 
		  sys.__stdout__.flush()
  
   	       # update currentRun
	       currentRun=run
	       
	    # seek to records in a key file only once when we open it
	    if currentKeyFileName!=keyFileName:
	       keyFile.seek(keyFileHeader[0])

   	    # update currentKeyFileName
	    currentKeyFileName = keyFileName
  
   	    # read syncValues from keyFile and write those which match into
   	    # output file
	    while 1:
	       try:
	          recordHeader = array.array('I')
	          recordHeader.fromfile(keyFile,6)
		  if needToSwap: recordHeader.byteswap()
	          #uid = 1 ## ???
	          syncValue = (int(recordHeader[0]),int(recordHeader[1]),int(recordHeader[2]))
#                  print "syncValue",syncValue
   		  if syncValue == sv:
		     recordHeader.tofile(outputFile)
                     counter+=1
		     break
   	       except EOFError:
	          break
	    processedSV=sv
	    sys.__stdout__.flush()

	keyFile.close()
	################# Fix key file header with correct number of SV's
	# seek to position where total SV's written
	outputFile.seek(0)
	nWordsArray = array.array('I')
	nWordsArray.fromfile(outputFile,2)
	pos = outputFile.tell()
	nSVHeader = array.array('I')
	nSVHeader.fromfile(outputFile,1)
	outputFile.seek(pos)
	nSVHeader[0]=counter
	nSVHeader.tofile(outputFile)
	#################
	if outputFile:
	   outputFile.close()
 
	# put generated key files into DB
	for x in outputList:
	    name   = x[0]
	    fileID = x[1]
	    run    = x[2]
	    uid    = x[3]
	    graphid   = x[4]
	    oFile  = x[5]

	    # first we need to close all opened files
	    oFile.close() 
	    # update FileID
	    typeId = self.updateFileType(name)
	    self.updateFileID(fileID,name,typeId)

	    # update KeyFile
            self.updateKeyFile(graphid,self.view,run,uid,fileID)

	# change permission
	os.chmod(outputFileName,0444)

	return self.ok

    def getMaxId(self):
        """Get MAX fileId from master ESDB. First we check MaxMasterID table for any
        recorded maxId. If it presents there we compare <db>@<host>:<port>:<socket>
        string to current one and raise exception if they're not match. If MaxMasterID
        is empty we retrieved information from master ESDB and record it to MaxMasterID.
        @rtype: long
        @return: maximum file id from the master db.
        """
        # check cache if we have master DB connector
	mkey  = (self.masterDBName,self.masterDBHost,self.masterDBPort,self.masterSocket) 
        db = dbType = cu = ""
        if self.masterCache.has_key(mkey):
           db,dbType = self.masterCache[mkey]
        else:
           db,dbType = es_init.connectToMasterDB(self.masterDBName,self.masterDBHost,self.masterDBPort,self.masterSocket,self.verbose)
           self.masterCache[mkey] = (db,dbType)
        cu  = db.cursor()
        query = "SELECT MAX(fileId) FROM FileID"
        cu.execute(query)
        tup = cu.fetchone()
        masterId = 0
        if tup and tup[0]:
           masterId = long(tup[0])

        # check if MaxMasterID table exists in current DB
        if self.dbType=="sqlite":
           query = "SELECT name FROM sqlite_master WHERE type='table'"
        else:
           query = "SHOW TABLES"
        tup = self.fetchAll(query)
        flag= 0
        for item in tup:
            if item[0]=="MaxMasterID": flag=1
        if not flag:
           self.createTables("MaxMasterID")
        # check if maxId were recorded to MaxMasterID table, if yes, just use it
        query = "SELECT * FROM MaxMasterID"
        tup   = self.fetchOne(query)
        currentMasterString = "%s@%s:%s:%s"%(self.masterDBName,self.masterDBHost,self.masterDBPort,self.masterSocket)
        if tup:
           maxId = long(tup[0])
           recordedMasterString= tup[1]
#           if recordedMasterString==currentMasterString:
#              if masterId>maxId:
#                 query="UPDATE MaxMasterID SET masterMaxId='%s' WHERE comment='%s'"%(masterId,currentMasterString)
#                 self.updateDBAndLog(query)
#                 maxId = masterId
#              if masterId!=maxId:
#                 print "Content of master DB has been changed which can cause Id overlap between DBs"
#                 print "Please re-start injection into new standalone DB"
#                 print "masterId='%s' maxId='%s'"%(masterId,maxId)
#                 raise "max fileId in master DB is different from record in MaxMasterDB"
#           else:
           if recordedMasterString!=currentMasterString:
              msg = "\nYour job has been used another master DB : %s"%recordedMasterString
              msg+= "\nCurrently, you are using the following DB: %s"%currentMasterString
              print msg
              raise "Failed to get maxId from master DB"
           
        # get max id from underlying DB
        ownMaxId = long(self.getIds(1))
        if ownMaxId>masterId:
           maxterId = ownMaxId
        maxId = masterId
        maxId*= 2

        # record maxId to the separate table if it doesn't exists before.
        query = "SELECT * FROM MaxMasterID"
        tup   = self.fetchOne(query)
        if not tup:
           query="INSERT INTO MaxMasterID (masterMaxId,comment) VALUES(%s,'%s')"%(maxId,currentMasterString)
           self.updateDBAndLog(query)
        return maxId

    def generateLocAndKeyFilesFrom(self,iFileList):
        """
	This routine generates location and key files from the given list of
        input sources. (So far, iFileList is a list of PDS files). 
        The input sources are analyzed and main ESDB or stand-alone DB is
	queried for the parents. Please note that we search for parents of the
	same data type as input source. For example:
          - there are no parents for pass2 injection since pass2 parents have a
	    different data type (binary)
          - there are parents for post-pass2, it is pass2.
        
        If parents are found (data files, key and location files), their
	key/location files can be used (instead of input data files) for
	building output location and key files.
        Otherwise, input data files are used. The common API uses 'readers' to
	walk through the files. Currently we implement two types of readers:
	pds reader (see PDSFileReader) and key/location files reader (see
	KeyLocFilesReader class).
        The reader access data information and navigates through data.
        
        In order to build output key/loc. files we auto-probe the input sources.
        If the parent's key file is found (and it is the only one), its sync
	values are compared to input sync values. If they differ input sources
	are declared as a skim (input sources are subset of parents, e.g.,
	DSkim).  In the case of a skim we use its list of sync values to drive
	the building process, otherwise parent's sync list is used. In the
	latter case, if the input source doesn't have sync value present in
	the parent sync values, fake entries are inserted into the output
	location files. In this case, the output key file is identical to
	parent one. In the case of a skim, the output key file contains a
	subset of the sync values wrt the parent sync values.
        
        To build the output location header:
	If dealing with a skim, we combine proxies from parents and
	input sources. e.g. DSkim injection.
	Otherwise we only use input source proxies, e.g. pass2 injection (no
	parents of the same data type are found) or post-pass2 injection (in
	this case parents are found (pass2), but sync values of post-pass2
	and pass2 are the same).
        
        @type  iFileList: list
        @param iFileList: list of input sources (pds files), e.g. qcd_hot.pds, 2photon_hot.pds
        @rtype: tuple (oFileDict,oKeyDict,oLocDict)
        @return: we return three dictionaries
           - dict[(run,uid)]=[(fileId,fileName,typeId,view)] this is a
             list of location files which include newly created loc. file and parents loc. files
           - dict[(run,uid)]=[(fileId,fileName,typeId)] this is a
             list of key files which include either newly created key file (in the case of skim)
             or key file of the parent (since it's cover the same sync. values) and parent's
             key files for different views, e.g. qcd view.
           - dict[fileId]=fileName, map of fileId vs data file names which include input 
             sources and their parents of the same type, e.g. when we inject post-pass2, 
             we return post-pass2 pds file and its parent's pass2 files.
        Those dictionaries are used to update FileID, KeyFile, Location and RunUID tables
        in L{updateDBUsingGroupList} method.
        """
	
        self.writeToLog("generateLocAndKeyFilesFrom")
        if self.verbose: print "Using generateLocAndKeyFilesFrom"
	
	masterKeyFileList  = [] # a list of master key files to be used as a driver
	#locParentList   =[]
	keyParentList   =[]
	#keyLocParentList=[]
	datParentList   =[]
	parentReaderDict={} # dict[childFileName]=list(parents)
	iSVRecordList   =[]
	fileList        =[]
	locDict         ={}
	datList         =[]
        transFileIdDict ={} # translation between stand-alone and master DB's
	combinedDict    ={}
	inputDataDict   ={}
	#datKeyLocDict   ={} # for one data file there is a ONE pair of key/loc. files
        oKeyDict        ={} # dict[(run,uid)]=[(fileId,fileName,typeId,view)]
        oLocDict        ={} # dict[(run,uid)]=[(fileId,fileName,typeId)]
        oFileDict       ={} # dict[fileId]=fileName
        parentsKeyFiles =[] # list of parents key files which need to be analyzed
                            # we keep this list for skim case, since those files should be modified
                            # wrt skim event list.
	for file in iFileList:
	    #cDict = build_pds_location.getStreamDataKeyDictFromPDS(file)
	    #for stream in cDict:
	    #    dataKeys=cDict[stream]
	    #    copyDataKeys=[]
	    #    for item in dataKeys:
            #copyDataKeys.append(item)
	    #    if combinedDict.keys().count(stream):
	    #	   existingList = combinedDict[stream]
	    #	   for item in copyDataKeys:
	    #	       if not existingList.count(item):
	    #		  existingList.append(item)
	    #	   combinedDict[stream] = existingList
	    #    else:
	    #	   combinedDict[stream]=copyDataKeys
	    fileList.append(file)
	    graphDict,parentDict,presentParents = self.findParents(file)
            # sort and reverse all graphid's from graphDict. They represents order how
            # parents files were injected in ESDB. Based on this order form datParentList.
            graphKeys = list(graphDict.keys())
            graphKeys.sort()
            graphKeys.reverse()
            for gid in graphKeys:
                parentList = graphDict[gid]
                for item in parentList:
                    datFile = item[1]
                    if not datParentList.count(datFile):
                       datParentList.append(datFile)
                       
	    if self.verbose:
	       print "parentDict",parentDict
	    if not parentDict and not presentParents:
	       for parent in self.parents: 
		   if parent and parent!='NULL':
	              print "You requested to inject data with the following parents:"
	 	      print parent
	              print "But toolkit was unable to find any parent in DBs. Please specify master DB."
	              return self.error

	    # re-structure parentDict into key,loc,dat parentList and form
	    # datKeyLocDict[dataFile]=locKeyReader
            # here key=(graphid,run,uid)
            # keyFileList=[fileId,fileName,typeId,view]
            # locFileList=[fileId,fileName,typeId]
            # locDataDict[keyFileName,locFileName]=[fileId,fileName,typeId]
	    for key in parentDict.keys():
                parentGid = key[0]
                parentRun = key[1]
                parentUid = key[2]
	        keyFileList,locFileList,locDataDict = parentDict[key]
                run = key[1]
                uid = key[2]
		#for item in locFileList:
		#    locId       = item[0]
		#    locFileName = item[1]
                #    typeId      = item[2]
                #    self.updateFileID(locId,locFileName,typeId)
                #    if not self.skim:
                #       oLocDict =gen_util.addToDict(oLocDict,(run,uid),locId)
                #    oFileDict=gen_util.addToDict(oFileDict,locId,locFileName)
		#    if not locParentList.count(locFileName):
		#       locParentList.append(locFileName)
                #    query = """SELECT id FROM Location WHERE graphid='%s' AND run='%s' 
                #    AND uid='%s' AND locationFileId='%s'"""%(parentGid,parentRun,parentUid,locId)
                #    tup   = self.fetchOne(query)
                #    if tup and tup[0]:
                #       if self.verbose:
                #          print "Found loc. parent info in DB"
                #    else:
                #       self.updateLocation(parentGid,parentRun,parentUid,locId)
		for item in keyFileList:
		    keyId       = item[0]
		    keyFileName = item[1]
                    typeId      = item[2]
		    view        = item[3]
                    # keep list of parents key files for skim case
                    if self.skim and view!='all':
                       parentsKeyFiles.append((parentGid,view,parentRun,parentUid,keyId,keyFileName))
                       continue
                    self.updateFileID(keyId,keyFileName,typeId)
                    if oKeyDict.has_key((run,uid)):
                       print "Found not unique key=('%s','%s') in key dictionary"%(run,uid)
                       return self.error
                    oKeyDict[(run,uid,view)]=(keyId,keyFileName)
                    oFileDict=gen_util.addToDict(oFileDict,keyId,keyFileName)
		    if view=="all":
		       if not keyParentList.count(keyFileName):
		          keyParentList.append(keyFileName)
		       if not self.skim:
		          masterKeyFileList.append((keyId,keyFileName))
                    query ="""SELECT keyFileId FROM KeyFile WHERE graphid='%s' AND view='%s' AND run='%s' AND uid='%s' AND keyFileId='%s'"""%(parentGid,view,parentRun,parentUid,keyId)
                    tup   = self.fetchOne(query)
                    if tup and tup[0]:
                       if self.verbose:
                          print "Found key. parent info in DB"
                    else:
                       if view=='all':
                          self.updateKeyFile(parentGid,view,parentRun,parentUid,keyId)
                       else:
                          if not self.skim:
                             self.updateKeyFile(parentGid,view,parentRun,parentUid,keyId)
		#for keyLocPair in locDataDict.keys():
                #    if self.verbose:
                #       print "For keyLocPair",keyLocPair
		#    listOfDataFiles = locDataDict[keyLocPair]
                #    for item in listOfDataFiles:
                #        fileId  = item[0]
		#	fileName= item[1]
                #        typeId  = item[2]
                #        if self.verbose:
                #           print fileId,fileName
                #        oFileDict=gen_util.addToDict(oFileDict,fileId,fileName)
                #        self.updateFileID(fileId,fileName,typeId)
		#        datKeyLocDict[fileName]=locKeyReader.LocKeyReader(keyLocPair[0],keyLocPair[1],fileId)
	        #        if not datParentList.count(fileName):
		#           datParentList.append(fileName)
	        
	    content = file_util.fileParser(file)
	    iSVRecordList += content[2] 
        
	if self.verbose:
	   gen_util.printListElements(iFileList,"input file list")
	   gen_util.printListElements(datParentList,"data parents")
	   #gen_util.printListElements(keyLocParentList,"key and loc parent files")
	   gen_util.printListElements(keyParentList,"key parent files")
	   #gen_util.printListElements(locParentList,"loc parent files")

	# build combined streamDataKey dictionary by parsing every location file we got:
	#locDict     = {}
	#for locFileName in locParentList:
	#    content = lpds_dump.locationFileParser(locFileName)
	#    # [streamNames,pdsIDList,oDict,hash,dataKeysInStreams,recordSize,posOfFirstRecord,needToSwap]
        #    streamDataKeyDict    = content[2]
	#    locDict[locFileName] = streamDataKeyDict
	#    for stream in streamDataKeyDict.keys():
	#	dataKeys = streamDataKeyDict[stream]
	#	if combinedDict.keys().count(stream):
	#	   existingList = combinedDict[stream]
	#	   for item in dataKeys:
	#	       if not existingList.count(item):
	#		  existingList.append(item)
	#	   combinedDict[stream] = existingList
	#        else:
	#	   combinedDict[stream] = dataKeys
	for key in combinedDict.keys():
	    myList = combinedDict[key]
	    myList.sort()
	    combinedDict[key]=myList

	# If I found more then one key parent file, it's a case of the skim and
	# I don't need to retrieve parent sv list.
	pSVRecordList=[]
	if len(keyParentList)==1:
	   keyFile = keyParentList[0]
#           print "keyFile",keyFile
	   pSVRecordList+=self.decodeKeyFile(keyFile)
        elif len(keyParentList)>1 and not self.skim and not self.noskim:
	   gen_util.printListElements(keyParentList,"Found %s key parent files"%len(keyParentList))
	   print "you're attempting to inject a skim, please use -skim option"
	   return self.error
	diffSet = set(pSVRecordList)-set(iSVRecordList)
        if len(diffSet) and not self.skim and not self.noskim:
	   print "Found different set of sync. values in provided input file list and its parents"
	   if self.verbose:
	      msg="Here the list of sync. values presented in parents, but missed in your input:"
	      diffList=[]
	      for item in diffSet:
	          diffList.append(item)
	      diffList.sort()
	      gen_util.printListElements(diffList,msg)
	   print "To resolve this issue, please either use -skim or -no-skim option"
	   return self.error
	if pSVRecordList and not set(pSVRecordList).issuperset(set(iSVRecordList)):
	   print "The parent sync. values are not superset of sync. values from provided list"
	   gen_util.printListElements(pSVRecordList,"parent list:")
	   gen_util.printListElements(iSVRecordList,"child list:")
	   return self.error
	
	skimSVRecordList   = [] # a list [(run,evt,uid,stream),...] which will use to navigate
	masterSVRecordList = [] # a list [(run,evt,uid,stream),...] which will use to navigate
	
	if self.skim:
	   svRecordList   = []
           for file in datParentList:
	       if not fileList.count(file):
	          fileList.append(file)
	   # we need to reverse the fileList, that parent will come first
           fileList.reverse()

           if self.verbose:
              gen_util.printListElements(fileList,"Final file list ")
	   # now we need to check if data overlap
	   # we don't want to check file presense in DB
#           checkFilesInDB=0
#           if self.error==self.allowStoreToDB(fileList,checkFilesInDB):
#              return self.error
	   skimSVRecordList=iSVRecordList
	else:
	   # collect svRecords from masterKeyFile list
	   if len(masterKeyFileList):
	      for item in masterKeyFileList:
		  keyFileName   = item[1]
		  masterSVRecordList += self.decodeKeyFile(keyFileName)
	
        # use maxId from stand-alone DB or from the Master
        # check if underlying DB is the same as master DB
        currentMasterString = "%s@%s:%s:%s"%(self.masterDBName,self.masterDBHost,self.masterDBPort,self.masterSocket)
        currentDBString = "%s@%s:%s:%s"%(self.dbName,self.dbHost,self.dbPort,self.dbSocket)
        if currentDBString!=currentMasterString:
           maxId = self.getMaxId()
           query="SELECT fileId FROM FileID WHERE fileId='%s'"%maxId
           tup  = self.fetchOne(query)
           if not tup:
              self.updateFileID(maxId,"",0)
        id = self.getIds(1)
	
	# declare variables to be used
	magicNumber      = 0xFFFFFFFFL # 2**32-1
	pdsIDList        = [] # list of fileId for all sources
	nPDSFiles        = len(fileList)
	iStreamNames     = [] # list of streams in original iFileList
	streamNames      = [] # list of streams found in all sources
	fakeFileIdList   = [] # list of fakes used for empty entries in loc file
	fileName         = "" # absolute file name
	fName            = "" # file name of new file
	keyFileId        = 0  # key file Id used for injection
	locFileId        = 0  # loc file Id used for injection
	lastEventNumber  = 0  # the last event number in all sources
	uid              = 0  # unique id of the run
	run              = 0  # run number
	lowerMostId      = 0  # lower 32-bit in uid
	upperMostId      = 0  # upper 32-bit in uid
	runUidList       = [] # return list of pairs (run,uid)
	fileDict         = {} # dict. of fileId:(pdsFile)
	fileDictUidRun   = {} # dict. of fileId:(uid,run)
	#fileDictStream   = {} # dict. of fileId:streamList
	runList          = [] # list of runs at current position of input files
        
        dictFileID_evt   = {} # dictionary which holds information about current
                              # record snapshot in given fileId.
	
	# please note that svList,listOfFileIds,fileOffsetList,recIndexList have to use
	# the same ordering
	svList           = [] # list of syncValues at current position of input files
	listOfFileIds    = [] # list of fileIds
	fileOffsetList   = [] # list of fileOffsets
	recIndexList     = [] # list of recordIndecies
	
	dataKeyFileIdDict= {} # dict of {'proxyName':[fileIds]}
	dataKeyStreamDict= {} # dict of {'proxyName':[streamName]}
	fileIdDataKeyDict= {} # dict of {fileId:[proxyNames]}
	streamDataKeyDict= {} # dict of {'streamName':[dataKey]}
	currentRun       = 999999        # max starting number for run
	#currentUid       = long(2**64+1) # max starting number for uid
	currentUid       = int(2**32+1) # max starting number for uid
        readerList = []
	print "\nProcessing files:"
	for idx in xrange(0,len(fileList)):
	    file = fileList[idx]
	    fName += string.split(os.path.split(file)[-1],".")[0]+"_"
	    fileID = self.getFileID(file)
	    if not fileID:
	       pdsIDList.append(id)
	       fileID = id
	       id = self.getIds(1)
	       if datParentList.count(file):
                  print "parent",file
               else:
	          print "input ",file
                  oFileDict=gen_util.addToDict(oFileDict,fileID,file)
	    else:
	       pdsIDList.append(fileID)
               oFileDict=gen_util.addToDict(oFileDict,fileID,file)
	    # if we have parent loc. file we'll use loc.HDDM reader, otherwise will use HDDM REST reader
#            reader = pds_dump.PDSFileReader(file)
            #if datKeyLocDict.has_key(file):
            #    reader = datKeyLocDict[file]   ### CHECK
            #else:
            fileType = file_util.fileType(file)
            if fileType == "rest":
                reader = hddm_r_dump.HDDMFileReader(file)
            elif fileType == "evio":
                reader = evio_dump.EVIOFileReader(file)
            else:
                print "no reader class found for file: " + file
                raise Error
            if not readerList.count((file,fileID,reader)):
                readerList.append((file,fileID,reader))
                fileDict[fileID] = reader

	    #sNames           = list(reader.streamNames())
	    #dataInStreams    = reader.dataInStreams()
            #sNames.sort()
            #
	    # check if file is a file from iFileList
	    #if iFileList.count(file):
	    #   for stream in sNames:
	    #       if not iStreamNames.count(stream):
	    #	      iStreamNames.append(stream)
	    # cross-checks, streamNames should remain the same among files
	    #if not len(streamNames):
	    #   streamNames = sNames
	    #if sNames != streamNames:
	    #   newList = []
	    #   if len(sNames)>len(streamNames):
	    #      newList = sNames
	    #   else:
	    #      newList = streamNames
	    #   if self.uniqueList(sNames) != self.uniqueList(streamNames):
            #      if self.verbose:
            #         print "Found different streams in:"
            #         print fileList[0],sNames
            #         print file,streamNames
            #         print "Will use",newList
            #   streamNames = newList
            #sNames = list(streamNames)
            #sNames.sort()
	
	    if self.verbose:
	       print "file",fileID,file
#            print "Trace file",fileID,file,sNames
            #fileDictStream[fileID]=sNames
	    # initialize array of file indexies
	    #for stream in sNames:
            #    if not dataInStreams.has_key(stream): continue
	    #    dataKeyList = dataInStreams[stream]
	    #	if not len(dataKeyList) and not streamDataKeyDict.has_key(stream):
	    #	   streamDataKeyDict[stream]=[]
	    #	for record in dataKeyList:
            #    # form recordFullName as: recordName+usageTag+productionTag
	    #	    recordFullName = record[0]+record[1]+record[2]
	    #	    if not streamDataKeyDict.has_key(stream):
	    #	           streamDataKeyDict[stream]=[recordFullName]
	    #	    else:
	    #	       if not streamDataKeyDict[stream].count(recordFullName):
	    #	           streamDataKeyDict[stream]+=[recordFullName]
	    #	    if not dataKeyFileIdDict.has_key(recordFullName):
	    #		   dataKeyFileIdDict[recordFullName]=[fileID]
	    #	    else:
	    #	           dataKeyFileIdDict[recordFullName]+=[fileID]
	    #	    if not dataKeyStreamDict.has_key(recordFullName):
	    #		   dataKeyStreamDict[recordFullName]=[stream]
	    #	    else:
	    #	           dataKeyStreamDict[recordFullName]+=[stream]

        if self.verbose:
           print "readerList",readerList
        # using readers from the list we read one event from the begining and end of input sources
        # (here input source can be either pds file or key/loc. pairs)
        # the last record info is used to find out lastEventNumber
        # the first record info is used to initialize all list which later used for walking
        # through the input sources (by using their readers)
        for file,fileID,reader in readerList:
	
            # read last record info
            syncValue,fileOffset = reader.readLastRecordInfo()

            if self.verbose:
               print "last  we read",syncValue,fileOffset,reader
            if syncValue:
                if not runList.count(syncValue[0]):
                   runList.append(syncValue[0])
                if self.skim:
                   if iFileList.count(file) and lastEventNumber<syncValue[1]:
                      lastEventNumber=syncValue[1]
                else:
                   if lastEventNumber<syncValue[1]:
                      lastEventNumber=syncValue[1]
            reader.backToFirstRecord()
               
	    # read first event from all sources to initialize sv, fileOffset, record lists
            syncValue,fileOffset = reader.readRecordInfo()
            dictFileID_evt[fileID]=(syncValue,fileOffset)
            
            # skip the file if it doesn't have any events
            if syncValue is None:
                continue

	    if syncValue and (run,uid)!=(syncValue[0],syncValue[2]):
	       lowerMostId,upperMostId=gen_util.lowerUpperBitsOfUID(syncValue[2])
	       run=syncValue[0]
	       uid=int(syncValue[2])
	       if not runUidList.count((run,uid)):
		  runUidList.append((run,uid))
            run = syncValue[0]
	    uid = int(syncValue[2])
            if run<currentRun:
	       currentRun=run
	    if uid<currentUid:
	       currentUid=uid
            fileDictUidRun[fileID]=(uid,run)
	    if self.verbose:
                print "first we read",syncValue,fileOffset,reader
	    if not runList.count(syncValue[0]):
	       runList.append(syncValue[0])
	    listOfFileIds.append(fileID)
	    svList.append(syncValue)
	    fileOffsetList.append(fileOffset)
	    #recIndexList.append(recIdx)

	    # read uid and check if we need to initialize lower(upper) bits
	    if syncValue and uid!=int(syncValue[2]):
	       lowerMostId,upperMostId=gen_util.lowerUpperBitsOfUID(syncValue[2])
	
	# sort all streams
        #sortedStreams = list(streamNames)
	#sortedStreams.sort()

	# sort all proxies in stream dictionary
	#maxNProxies=0
	#for stream in streamDataKeyDict.keys():
	#    dlist = streamDataKeyDict[stream]
	#    dlist.sort()
	#    if len(dlist)>maxNProxies:
	#       maxNProxies=len(dlist)
	#    streamDataKeyDict[stream]=dlist

	# form combined fileName for key/location file
	minRun=min(runList)
	maxRun=max(runList)
	if minRun!=maxRun:
	   fileName="run%s-run%s"%(minRun,maxRun)
	else:
	   fileName="run%s"%minRun
	fileName=self.oDir+fileName
	if self.verbose: 
	   print "Combined fileName", fileName
	   print "We start from uid='%s' and run='%s'"%(currentUid,currentRun)

	# calculate based on maxNProxies how many ASU's we should have to store
	#nASUperRecord = maxNProxies
	#while((nASUperRecord + 4) % 8):
	#    nASUperRecord +=1
	
	################## Make location file header
	# use combined proxy-dictionary of input sources
        # - when there is no parents
        # - when we're dealing with skim case
        # use input source proxy-dictionary
        # - when dealing with case where parents are found, e.g. post-pass2
	#if self.skim or not len(datParentList):
	#   newLocHeader = build_pds_location.buildLocationHeaderFromDict(combinedDict,pdsIDList)
	#else:
	#   newLocHeader = build_pds_location.buildLocationHeaderFromDict(cDict,pdsIDList)
	#combinedLocationHeader=newLocHeader
	#charArrayOfStreams=pds_utils.charArrayOfStreams(streamNames).tolist()

	################## Write location file header
	#locFileName = fileName+"-esdb-%d.lpds"%id
	#locFileId   = id
        #oFileDict=gen_util.addToDict(oFileDict,locFileId,locFileName)
	#locFile     = open(locFileName,"wb")
	#combinedLocationHeader.tofile(locFile)
   
	# get new keyFileId
	id = self.getIds(1)
	keyFileId = id

	################## Make key file header
	#nWordsInCharArrayOfStreams=len(charArrayOfStreams)
	#nWordsInHeader= 7-3+nWordsInCharArrayOfStreams
	#nNames        = len(streamNames)
	headerList    = [2718281*256,keyFileId]
	#headerList   += [len(charArrayOfStreams)]+charArrayOfStreams
	posOfTotalSV  = len(headerList)
	# so far we'll write magicNumber in place of totalSVs, later we'll re-write it
	headerList   += [magicNumber]
	SIGNATURE     = 2718281
	#keyFileName   = fileName+"-esdb-%d.ikey"%keyFileId
	keyFileName   = fileName+"-esdb.ikey"
	keyFile       = open(keyFileName,'w+b') # we need to read(for seek) & write
	headerHeader  = array.array('I')
	headerHeader.fromlist(headerList)
	headerHeader.tofile(keyFile)
	recordHeader  = []
	################# End of key file header
        print "Open keyFile",keyFileName
	
	################# New loop
	# open all sources and advance index if next SV is different and stream
	# has been changed. If all sources have the same SV write only location
	# info and postpone key file update.
	
	masterIdx    = 0 # current index of master svRecordList
	prevEvt      =-1 # keep track of previous event
	recordList   = [magicNumber]*len(streamNames)
	locCounter   = 0 # counter of sync values written to loc. file
	nSources     = len(fileList)
	nSyncValues  = 0 # count number of processed syncValues
	nSVInKeyFile = 0
	combinedFileOffsetList = [0]*nPDSFiles*2
	onHoldUidRunList = [] # list of (fileId,uid,run) triplets on hold
	onHoldFileIdList = [] # list of fileIds on hold
	for id in fileDict.keys():
	    uidRunPair = fileDictUidRun[id]
	    if uidRunPair!=(currentUid,currentRun):
	       onHoldUidRunList.append(uidRunPair)
	       onHoldFileIdList.append(id)
	fileId=0              # fileId is file Id we currently use
	initRecIdx=0          # index of first record in a file 
	nonExistingIndex=-MAGIC_NUMBER
	currentStream=""      # stream name we'are currently in
	# initialize record index, fileId, currentStream, uid of the first source we will from
	for itemIdx in xrange(0,len(svList)):
	    sv=svList[itemIdx]
	    if not onHoldUidRunList.count((sv[2],sv[0])):
#	       initRecIdx   = recIndexList[itemIdx]
	       fileId       = listOfFileIds[itemIdx]
#               currentStream= streamNames[initRecIdx]
#	       currentStream= sv[3]
#               print "currentStream",currentStream,sv
	       lowerMostId,upperMostId=gen_util.lowerUpperBitsOfUID(sv[2])
	if self.verbose:
	   print "onHoldUidRunList",onHoldUidRunList
	   print "fileDict.keys",fileDict.keys()
	   print "listOfFileIds",listOfFileIds,"we start with fileId",fileId
           print "dictFileID_evt",dictFileID_evt
	   print "svList",svList
#	   print "recIndexList",recIndexList,"initRecIdx",initRecIdx
	   print "fileOffsetList",fileOffsetList
	   print "combinedFileOffsetList",combinedFileOffsetList
	   #print "streamNames",streamNames
	   #print "currentStream",currentStream

        
        #streamPriorityList=['beginrun','startrun','event','endrun']
	#prevRecIdx    = initRecIdx # record index which was written to loc. file
	#nextRecIdx    = prevRecIdx # record index which we will write out next
	writtenEvt    =-1
	currentEvt    =-1
	#currentRecIdx =-1
	#currentRecIdx = nonExistingIndex
	skimSVidx     = 0 # index of current position in skimSVRecordList
        # array holding file indecies for data proxies in a stream
	#fileIdList    = [255]*nASUperRecord 
        fakeSVList    = [] # list of fake sv's
        nCollectedProxiesInStream = 0 # number of proxies we read from all sources in a stream
	nCollectedFileIds         = []# list of file ids which we collect to write  
	counter=0
	while 1:
		 
#           if counter>100:
#              break
#           counter+=1
	   # check if onHoldUidRunPair list size is equal the size of the sources
	   # if so, it's time release the hold on this pair
	   if len(onHoldUidRunList)==nSources:
	      if self.verbose:
	         print "Cleanup onHoldUidRunList",onHoldUidRunList,nSources
	      onHoldUidRunList=[]
	      #onHoldFileIdList=[]
	      #prevRecIdx=initRecIdx

	   # if we fail to read from all available sources, time to exit global loop
	   if not nSources:
	      if self.verbose:
	         print "We read all sources, time to quit"
	      break
	  
	   # find smallest sv in non-advances valid (not on hold) sources
	   evt = magicNumber
	   idx = magicNumber
	   smallestSV = ()
#           print
#           print "old evt",evt,"idx",idx,"fileId",fileId,"currentStream",currentStream
#           print "svList",svList
	   validRecCounter=0 # counter of valid record indecies
	   leftRecCounter=nSources-len(onHoldUidRunList)
	   minSV = ()
	   for svIdx in xrange(0,len(svList)):
	       readSV = svList[svIdx]
#               print "read",readSV,svIdx,listOfFileIds[svIdx],fileId
	       try:
	          uidRunPair = (readSV[2],readSV[0])
	       except:
	          uidRunPair = ()
	       # if we found that (uid,run) pair is marked on hold, skip it
	       if onHoldUidRunList.count(uidRunPair):
		  continue
	       ###########
	       # if file doesn't contain currentStream, skip it, but count it since it's active file
	       #if not fileDictStream[listOfFileIds[svIdx]].count(currentStream):
	       #   if self.verbose:
	       #      print "Skip fileId=%s since it doesn't contain '%s' stream"%(listOfFileIds[svIdx],currentStream)
	       #	  validRecCounter+=1
	       #	  continue
	       ###########
	       sv = readSV
	       # if we read empty syncValue (EOF assignment)
	       if not sv:
                  continue
	       ###########
	       # keep track of minimal SV in a valid sv's
	       if not minSV:
	          minSV=sv
	       if minSV>sv:
	          minSV=sv
#               print "indecies",svIdx,recIndexList[svIdx],prevRecIdx,recIndexList
#               print "current='%s' s_rec='%s' s_prev='%s'"%(currentStream,streamNames[recIndexList[svIdx]],streamNames[prevRecIdx])
#               print "sv",sv
	       validRecCounter+=1
#	       if sv[3]!=currentStream:
#	          continue
#               print "sv which pass",sv
	       if evt>sv[1] or sv[1]==magicNumber:
                   fileId         = listOfFileIds[svIdx]
                   #currentRecIdx  = recIndexList[svIdx]
                   currentEvt     = sv[1]
                   idx            = svIdx
                   evt            = sv[1]
                   smallestSV     = sv
                   leftRecCounter-=1
	      
	   # if all sources are at the same stream, search smallest among them
	   if leftRecCounter==validRecCounter: # if all events are the same
	      svIdx         = svList.index(minSV)
	      fileId        = listOfFileIds[svIdx]
	      #currentRecIdx = recIndexList[svIdx]
	      idx           = svIdx
	      sv            = svList[idx]
	      evt           = sv[1]
	      currentEvt    = sv[1]
	      smallestSV    = sv
#           print "new evt",evt,"idx",idx,"fileId",fileId,"validRecCounter",validRecCounter,"leftRec",leftRecCounter
	   if self.verbose:
	      print "Smallest events among all sources",smallestSV,"fileId",fileId
              print "All sources:"
              for sv in svList: print sv
           idxToWrite = 0

           ### now we we actually start building the corresponding key file
           # if we got masterKeyFileList we need to compare everytime do we                                    
           # need to write fake info or not. This is happens when masterKeyFile                                
           # cover more events then the source                                                                 
           if len(masterKeyFileList):
               # compare found sv with one from master key file                                                 
               startIdx = masterIdx
               for idx in xrange(startIdx,len(masterSVRecordList)):
                   svRecord  = masterSVRecordList[idx]
                   masterIdx = idx
                   masterSV  = (svRecord[0],svRecord[1],svRecord[2]) # (run,evt,uid)                            
                   #masterRec = svRecord[3] # stream name                                                        
                   # if runs/events are equal do nothing
                   sv = svList[idxToWrite]
                   if self.verbose:
                       print "master",svRecord,"currentSV",sv,svList
                   if nSources and svList[idxToWrite][0]==masterSV[0]:
                       if svList[idxToWrite][1]==masterSV[1]:
                           masterIdx = idx+1
                           break
                       elif masterSV[1]>svList[idxToWrite][1]:
                           if self.skim:
                               continue
                           print "ERROR: masterSV>currentSV"
                           print "masterSV",masterSV
                           print "svList",svList[idxToWrite]
                           print "masterList"
                           print masterSVRecordList
                           print "svList"
                           print svList
                           print "masterKeyFileList:",masterKeyFileList
                           return self.error

           # prepare what we're going to write                                                                 
           w_sv      = svList[idxToWrite]
           #w_recIdx  = recIndexList[idxToWrite]

           usedListOfFileIds=""

           # write to key file only if current event                                                              
           # has been changed wrt previous one                                                                    
           ########################                                                                               
           # write to key file                                                                                    
           ########################                                                                               
           ##if prevEvt!=-1 and prevEvt!=writtenEvt:  ## DO WE NEED THIS?
           recordArray = array.array('I')
           recordArray.fromlist([w_sv[0],w_sv[1],w_sv[2],1,fileOffset>>32,fileOffset&0xffffffff])  ### actually set data here!!
           recordArray.tofile(keyFile)
           nSVInKeyFile+=1
           keyFile.flush()
           ########################                                                                               
           writtenEvt = w_sv[1]

           # finally update previous event counter                                                                
           #prevEvt = writtenEvt
           #continue
           ####################################################################                                      
           # current record index we're processing                                                                   
           # compare it to the next record index we intend to write                                                  
           # advance the source if current record is different from next one                                         
           reader = fileDict[fileId]
           syncValue,fileOffset = reader.readRecordInfo()
           dictFileID_evt[fileId]=(syncValue,fileOffset)
           if not syncValue:
               break
           #if recordStream:
           #    recIdx = sNames.index(recordStream)

           uid,run = fileDictUidRun[fileId]
           #recIndexList[idx]=recIdx
           newUidRunPair=(0,0)
           if syncValue:
               newUidRunPair= (syncValue[2],int(syncValue[0]))
               # update fileOffset only if we read valid sync value                                                
               fileOffsetList[idx]=fileOffset
           if (uid,run)!=newUidRunPair:
               if self.verbose:
                   print "uid/run pair is changed from",(uid,run),"to",newUidRunPair
               fileDictUidRun[fileId]=newUidRunPair
               if syncValue:
                   onHoldUidRunList.append(newUidRunPair)
                   onHoldFileIdList.append(fileId)
           try:
               r,e,u=syncValue
               offset=fileOffset
               #offset=gen_util.form64BitNumber(fileOffset[0],fileOffset[1])
               #readStream=fileDictStream[fileId][recIdx]
               # when there is multiple stream, e.g. startrun, once it's found                                     
               # in skim file and it has highest priority we need to jump to it                                    
               #if self.skim and streamPriorityList.count(readStream) and \
               #        streamPriorityList.index(readStream)<streamPriorityList.index(currentStream):
               #    currentStream=readStream
           except:
               r,e,u=('Na','Na','Na')
               offset='Na'
               #readStream='Na'
           if self.verbose:
               print "+++++ we read locCounter=%s sv=(%s,%s,%s) fileOffset=%s from fileId=%s"%(locCounter,r,e,u,offset,fileId)
               print "fileOffsetList",fileOffsetList
               print
           if not syncValue:
               # we found EOF while reading next record                                                            
               nSources-=1
           # read uid and check if we need to initialize lower(upper) bits                                        
           if syncValue and (uid,run)!=newUidRunPair:
               lowerMostId,upperMostId=gen_util.lowerUpperBitsOfUID(syncValue[2])
               run=syncValue[0]
               uid=int(syncValue[2])
               if not runUidList.count((run,uid)):
                   runUidList.append((run,uid))
           if syncValue:
               svList[idx]=syncValue ##+(fileDictStream[fileId][recIdx],)
           else:
               svList[idx]=()
           #if nextRecIdx==magicNumber:
           #    nextRecIdx=recIdx
        ####################################################################                                      


	   
	################# End loop
        #locFile.close()
	# we need to write last buffer into key file
	recordArray = array.array('I')
	recordArray.fromlist(recordHeader)
	recordArray.tofile(keyFile)
	#nSVInKeyFile+=1
	keyFile.flush()
	################# Fix key file header with correct number of SV's
	# seek to position where total SV's written
        nWordsInHeader = 3
	keyFile.seek((nWordsInHeader-1)*4,0)
	pos = keyFile.tell()
	nSVHeader = array.array('I')
	nSVHeader.fromfile(keyFile,1)
	if nSVHeader[0]!=magicNumber:
	   print "Miss position of nSyncValues in a file"
	   return self.error
	keyFile.seek(pos)
	nSVHeader[0]=nSVInKeyFile
	nSVHeader.tofile(keyFile)
	keyFile.close()
	#################
	# make a cross check that last writtenEvt is the one
        #  NOTE: come back and check this later
        #if lastEventNumber!=writtenEvt:
        #   print "While building combined key/location file, got async"
        #   print "nSyncValues processed:",locCounter
        #   print "nSyncValuesInKeyFile :",nSVInKeyFile
        #   print "Last event found     :",lastEventNumber
        #   print "Last event written   :",writtenEvt
        #   return self.error
        # loop over parentsKeyFiles and correct them wrt new keyFile
        if self.skim:
           for parentGid,view,parentRun,parentUid,keyId,parentKeyFileName in parentsKeyFiles:
               newFileId     = self.getIds(1)
#               newFileName   = key_dump.stripKeyFile(keyFile.name,newFileId,keyFileName)
               newFileName   = key_dump.stripKeyFile(parentKeyFileName,keyFile.name,newFileId,keyFileName)
#               print parentGid,view,parentRun,parentUid,keyId,parentKeyFileName,"created",newFileId,newFileName
               if self.verbose:
                  print "Created new key file for view='%s'"%view
                  print newFileName
               oKeyDict[(parentRun,parentUid,view)]=(newFileId,newFileName)
               oFileDict=gen_util.addToDict(oFileDict,newFileId,newFileName)

	if self.verbose:
	   print "### runUidList",runUidList
	# update list of file which we suppose to commit to DB
        #typeId = self.updateFileType(locFileName)
        #for pair in runUidList:
        #    oLocDict=gen_util.addToDict(oLocDict,pair,locFileId)
	#os.chmod(locFileName,0444) # set permission to '-r--r--r--'

        # there are three cases when we need to keep newly created key file
        # - if input is a skim
        # - if no key parent file found
        # - if we insert fakes into loc. file and need to recreate key file(s)
        if self.skim or not len(keyParentList) or len(fakeSVList):
           if len(fakeSVList):
              for key in oKeyDict.keys(): # key=(run,uid,view)
                  fileid,name = oKeyDict[key]
                  if key[2]==self.view:
                     newFileId     = keyFileId
                     newFileName   = keyFileName
                  else:
                     newFileId     = self.getIds(1)
                     newFileName   = key_dump.recreateKeyFile(name,newFileId,fakeSVList)
                  oKeyDict[(key[0],key[1],key[2])]=(newFileId,newFileName)
                  oFileDict=gen_util.addToDict(oFileDict,newFileId,newFileName)
           else:
              for run,uid in runUidList:
                  oKeyDict[(run,uid,self.view)]=(keyFileId,keyFileName)
              oFileDict=gen_util.addToDict(oFileDict,keyFileId,keyFileName)
	   os.chmod(keyFileName,0444)
        else:
           os.remove(keyFileName)

#        print "Print oFileDict"
#        for key in oFileDict.keys():
#            print key,oFileDict[key]
#        print "Print oKeyDict"
#        for key in oKeyDict.keys():
#            print key,oKeyDict[key]
        return (oFileDict,oKeyDict,oLocDict)


#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""SQLUtil class defines high-level API for EventStore tables.
Use this class to create/drop/access/print content of ES tables."""

import os, sys, string, time
import os_path_util, gen_util

class SQLUtil:
    """SQLUtil class defines high-level API for EventStore tables."""
    def __init__(self,db,dbType,dbLog=""):
	self.db     = db
#        self.cursor = self.db.cursor()
	self.logFile= dbLog
	self.pid    = os.getpid()
	self.commitFlag=1
	if dbLog:
	   self.log= dbLog 
	else:
	   self.log= os_path_util.NullDevice()
	self.comp  = "" # compensate file
	self.schemaDict = {}
	self.dbType= dbType
	if self.dbType=="sqlite":
	   self.UINT  = "INTEGER"
	   self.INT   = "INTEGER"
	   self.long  = "INTEGER NOT NULL"
	   self.uid   = "TEXT NOT NULL" # to avoid SQLite3 supports w/ 64-bit numbers
	else:
	   self.UINT  = "INT UNSIGNED"
	   self.INT   = "INT"
	   self.long  = "BIGINT UNSIGNED NOT NULL"
	   self.uid   = "BIGINT UNSIGNED NOT NULL"
	self.verbose  = 0
	# get db table names from DB
	if dbType=="sqlite":
	   query="SELECT name FROM sqlite_master WHERE type='table';"
	elif dbType=="mysql":
	   query="SHOW TABLES"
	self.startTxn("SQLUtil.__init__")
	tup=self.fetchAll(query)
	self.endTxn("SQLUtil.__init__")
	self.dbNames=[]
	for item in tup:
	    self.dbNames.append(item[0])
	self.dbid   = []
	for idx in xrange(0,len(self.dbNames)):
	    self.dbid.append(idx)

    def setIsolationLevel(self,level=None):
	"""Set isolation level for internal DB (necessary to perform SQLite
	transactions."""
	self.db.isolation_level=level

    def getIsolationLevel(self):
	"""Get isolation level of SQLite DB"""
	return self.db.isolation_level

    def setCommitFlag(self,flag=1):
        """Set flag which will inform underlying DB what to do with transaction commits."""
        self.commitFlag=flag
	
    def lockTables(self,tableName=""):
        """Lock tables in EventStore. If no tableName is provided, then lock all tables"""
        tables = ""
	for table in self.dbNames:
	    if not tables:
	       tables+='%s WRITE'%table
	    else:
               tables+=',%s WRITE'%table
        if tableName:
	   tables='%s WRITE'%tableName
        if self.dbType=='mysql':
	   query="LOCK TABLES %s"%tables
	elif self.dbType=='sqlite':
	   # in this case we don't need to pass any query since DB should be opened
	   # with EXCLUSIVE isolation level
	   query=""
	if query:
	   self.updateDBAndLog(query)
	
    def unlockTables(self):
        """Unlock all locked tables in EventStore"""
        if self.dbType=='mysql':
           query = "UNLOCK TABLES" 
	   self.updateDBAndLog(query)
	elif self.dbType=='sqlite':
	   self.db.commit()
    #
    # SQLite doesn't like nested transactions, we will use them only
    # for MySQL
    def startTxn(self,msg=""):
        """Update EventStore db log and invoke BEGIN transaction. In the case
           of SQLite we rely on IMMEDIATE transaction mechanism which locks DB for
	   that transaction."""
	if self.verbose and msg:
	   message ="Start transaction: "+msg
	   message+=", commit flag=%s"%self.commitFlag
	   if self.dbType=='sqlite':
	      message+=", %s"%self.db.isolation_level
	   print message
	   sys.__stdout__.flush()
	if self.dbType=="mysql":
	   if self.commitFlag:
	      self.updateDBAndLog("BEGIN")
	elif self.dbType=="sqlite":
	   if not self.commitFlag:
	      return
	   if self.db.isolation_level:
	      return
	   counter=0
	   query = "BEGIN IMMEDIATE"
	   cu = self.db.cursor()
	   while 1:
	       if counter>10: break
	       try:
		  cu.execute(query)
#                  self.cursor.execute(query)
	          self.updateLog(query)
	          break
	       except:
	          print "Wait for BEGIN"
		  gen_util.printExcept()
		  pass
	       counter+=1
	   cu.close()
	   return
	   
    def endTxn(self,msg=""):
        """Update EventStore db log and invoke COMMIT transaction. In the case
           of SQLite we use DB-API db.commit() to commit our transactions. Please note
           that SQLite only support DB locking."""
	if self.dbType=="mysql":
	   if self.commitFlag:
	      self.updateDBAndLog("COMMIT")
	elif self.dbType=="sqlite":
	   if self.commitFlag:
	      if self.db.isolation_level:
		 return
	      query="COMMIT"
	      counter=0
	      cu = self.db.cursor()
	      while 1:
	          if counter>10: break
		  try:
                      self.db.commit()   ## sdobbs, 4/19/14
#		     cu.execute(query)
#                     self.cursor.execute(query)
                      self.updateLog(query)
                      break
	          except:
		     print "Wait for COMMIT"
		     gen_util.printExcept()
		     pass
	          counter+=1
	      cu.close()

#              self.db.commit()
#              cu = self.db.cursor()
#              cu.execute(query)
#              cu.close()
#              self.db.commit()
#              self.updateLog(query)
	
	if self.verbose and msg:
	   message = "  End transaction: "+msg
	   message+=", commit flag=%s"%self.commitFlag
	   if self.dbType=='sqlite':
	      message+=", %s"%self.db.isolation_level
	   print message
	   print
	   sys.__stdout__.flush()

    def idx(self,name):
        return self.dbNames.index(name)
	
    def writeToLog(self,msg):
        """Update EventStore db log with time/pid/msg signature"""
	localtime = "%s "%time.strftime("%H:%M:%S",time.localtime())
	pid       = "%s "%self.pid
        self.log.write(pid+localtime+'###### '+msg+'\n')
	
    def getTableNames(self):
        """Return a list of table names in EventStore DB"""
	if not self.dbNames:
	   # get db table names from DB
	   if self.dbType=="sqlite":
	      query="SELECT name FROM sqlite_master WHERE type='table';"
	   elif self.dbType=="mysql":
	      query="SHOW TABLES"
	   self.startTxn("SQLUtil.__init__")
	   tup=self.fetchAll(query)
	   self.endTxn("SQLUtil.__init__")
	   for item in tup:
	       self.dbNames.append(item[0])
	return self.dbNames
	
    def commit(self):
	"""Explicit commit"""
	self.db.commit()
    def rollback(self):
	"""Rollback transaction"""
	self.db.rollback()
    def close(self):
	"""Close cursor and db connector"""
#        self.cursor.close()
	self.db.close()
    def setVerboseLevel(self,verbose):
	"""Set verbose level, 0 low, 1 is high. In future may verbose levels
	can be added."""
	self.verbose=verbose
	if self.verbose and self.dbType=='sqlite':
	   print "db isolation",self.db.isolation_level

    def getLastId(self,table):
	"""Get last autoincemented id for given table"""
	if self.dbType=="sqlite":
	   query = "SELECT LAST_INSERT_ROWID() FROM %s LIMIT 1"%table
	else:
	   query = "SELECT LAST_INSERT_ID() FROM %s"%table
	localtime= "%s "%time.strftime("%H:%M:%S",time.localtime())
	pid      = "%s "%os.getpid()
        self.log.write(pid+localtime+query+'\n')
	# since SQLite locks a whole table we use separate cursor to get
	# information while transaction still in progress
	cur = self.db.cursor()
	cur.execute(query)
	tup = cur.fetchone()
	id  = tup[0]
	cur.close()
#        tup = self.fetchOne(query)
	id  = tup[0]
        return id
	
    def updateLog(self,iQuery,cQuery=None):
	"""Update EventStore log and execute given query.
	The log is written in the following format:
	hh:mm:ss pid query"""
        query     = string.join(string.split(iQuery))
	localtime = "%s "%time.strftime("%H:%M:%S",time.localtime())
	pid       = "%s "%os.getpid()
        self.log.write(pid+localtime+query+'\n')
	if cQuery: 
           cq = string.join(string.split(cQuery))
	   if self.comp: self.comp.write(cq+'\n')
	return query
	   
    def updateDBAndLog(self,iQuery,cQuery=None):
	query = self.updateLog(iQuery,cQuery)
	cur   = self.db.cursor()
	try:
	   cur.execute(query)
	   self.db.commit()
#           self.cursor.execute(iQuery)
	except:
	   localtime = "%s "%time.strftime("%H:%M:%S",time.localtime())
	   print "%s Fail to execute \n%s\n"%(localtime,query)
	   cur.close()	
	   raise
	cur.close()	
	
    def fetchOne(self,query):
	"""Update a EventStore log and retrieve one row for given query"""
#        self.updateDBAndLog(query)
#        tup = self.cursor.fetchone()
	self.updateLog(query)
	cur = self.db.cursor()
	cur.execute(query)
	tup = cur.fetchone()
	cur.close()
	return tup
	
    def fetchAll(self,query):
	"""Update a EventStore log and retrieve all rows for given query"""
#        self.updateDBAndLog(query)
#        tup = self.cursor.fetchall()
	
	self.updateLog(query)
	cur = self.db.cursor()
	cur.execute(query)
	tup = cur.fetchall()
	cur.close()
	return tup
	
    def dropTable(self,table):
	"""Drop table in EventStore"""
        query = "DROP TABLE "+table
	cur = self.db.cursor()
	try:
	   iQuery = self.updateLog(query)
	   cur.execute(iQuery)
#           self.cursor.execute(iQuery)
	except:
	   self.log.write("No %s table found\n"%table)
	cur.close()

    def createTables(self,table="all"):
        """Create table(s) in EventStore database. For complete list of
	tables please visit: 
	https://wiki.lepp.cornell.edu/CleoSWIG/bin/view/Main/EventStoreDesign"""
        auto=""
	if self.dbType=="mysql":
	   auto="AUTO_INCREMENT"
	   
	tableName="FileID"
        if table=="all" or table==tableName:
	   # Drop/create FileID table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """CREATE TABLE %s (
	   fileid %s %s PRIMARY KEY, 
	   fileName TEXT,
	   typeid %s
	   )
	   """%(tableName,self.long,auto,self.UINT)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query

	tableName="KeyFile"
	if table=="all" or table==tableName:   
	   # Drop/create KeyFile table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """
	   CREATE TABLE %s (
	   graphid %s NOT NULL, 
	   view VARCHAR(255) NOT NULL, 
	   run %s NOT NULL, 
	   uid %s, 
	   keyFileId %s NOT NULL, PRIMARY KEY(graphid,view,run,uid) )
	   """%(tableName,self.UINT,self.UINT,self.uid,self.UINT)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query
	
	tableName="RunUID"
        if table=="all" or table==tableName:
	   # Drop/create RunUID table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """
	   CREATE TABLE %s (
	   run %s NOT NULL,
	   uid %s )
	   """%(tableName,self.UINT,self.uid)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query

	tableName="MaxMasterID"
        if table==tableName:
	   # Drop/create RunUID table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """
	   CREATE TABLE %s (
	   masterMaxId %s NOT NULL,
	   comment TEXT )
	   """%(tableName,self.UINT)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query

	tableName="Location"
        if table=="all" or table==tableName:
	   # Drop/create Localtion table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """
	   CREATE TABLE %s (
	   id %s %s PRIMARY KEY,
	   graphid %s NOT NULL, 
	   run %s NOT NULL, 
	   uid %s, 
	   locationFileId %s NOT NULL )
	   """%(tableName,self.long,auto,self.UINT,self.UINT,self.uid,self.UINT)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   query = "CREATE INDEX LocationGroups ON Location(graphid,run,uid)"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query

	tableName="Version"
        if table=="all" or table==tableName:
	   # Drop/create Version table in SQLDB.EventStoreDB
	   self.dropTable(tableName)
	   query = """
	   CREATE TABLE %s (
	   id %s %s PRIMARY KEY,
	   grade VARCHAR(255) NOT NULL, 
	   timeStamp %s NOT NULL, 
	   minRunNumber %s NOT NULL, 
	   maxRunNumber %s NOT NULL, 
	   graphid %s NOT NULL,
	   state VARCHAR(10) ) 
	   """%(tableName,self.long,auto,self.UINT,self.UINT,self.UINT,self.UINT)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query

	tableName="SpecificVersion"
        if table=="all" or table==tableName:
	   # Drop/create SpecificVersion table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """
	   CREATE TABLE %s (
	   svName VARCHAR(255) NOT NULL PRIMARY KEY, 
	   svid %s NOT NULL )
	   """%(tableName,self.UINT)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query

	tableName="SpecificVersionComment"
        if table=="all" or table==tableName:
	   # Drop/create SpecificVersionComment table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """
	   CREATE TABLE %s (
	   id %s %s NOT NULL PRIMARY KEY,
	   svid %s NOT NULL,
	   CommentDate %s,
	   Comment TEXT )
	   """%(tableName,self.UINT,auto,self.UINT,self.UINT)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query

	tableName="GraphPath"
        if table=="all" or table==tableName:
	   # Drop/create GraphPath table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """
	   CREATE TABLE %s (
	   graphid %s NOT NULL PRIMARY KEY, 
	   svid %s NOT NULL )
	   """%(tableName,self.UINT,self.UINT)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query

	tableName="PathDepend"
        if table=="all" or table==tableName:
	   # Drop/create GraphPath table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """
	   CREATE TABLE %s (
	   parentId %s, 
	   childId %s NOT NULL )
	   """%(tableName,self.UINT,self.UINT)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query

	tableName="FileType"
        if table=="all" or table==tableName:   
	   # Drop/create FileType table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """CREATE TABLE %s (
	   id %s %s PRIMARY KEY, 
	   type VARCHAR(8) NOT NULL,
	   description TEXT )
	   """%(tableName,self.UINT,auto)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query
	
	tableName="OrphanFileID"
        if table=="all" or table==tableName:
	   # Drop/create FileType table in SQLDB.EventStore
	   self.dropTable(tableName)
	   query = """CREATE TABLE %s (
	   id %s PRIMARY KEY, 
	   dateTime DATETIME,
	   user VARCHAR(8) NOT NULL )
	   """%(tableName,self.long)
	   if self.dbType=="mysql": query+=" type=innodb"
	   self.updateDBAndLog(query)
	   if not self.schemaDict.has_key(tableName):
	      self.schemaDict[tableName]=query
	
    def getTables(self):
	"""Return list of tables from DB"""
	return self.dbNames
#        if self.dbType=="sqlite":
#           query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
#           tup   = self.fetchAll(query)
#        else: # MySQL 
#           query = "SHOW TABLES"
#           tup   = self.fetchAll(query)
#        tableList= []
#        for item in tup:
#            tableList.append('%s'%item[0])
#        return tableList

    def getTableSchema(self,tableName):
	"""Return EventStore table schema"""
	if not self.schemaDict.has_key(tableName):
	   if self.dbType=="sqlite":
	      query = "SELECT * FROM sqlite_master WHERE name='%s'"%tableName
	      tup   = self.fetchOne(query)
	      schema= tup[4]
	   else: # MySQL 
	      query = "DESCRIBE %s"%tableName
	      tup   = self.fetchAll(query)
	      schema= "CREATE TABLE %s ("%tableName
	      for item in tup:
	          name = item[0]
		  type = item[1]
		  priKey = item[3]
		  autoInc = item[5] 
	          schema+=name+' '+type+' '+priKey+' '+autoInc
		  if item!=tup[-1]:
		     schema+=','
	      schema+=" )"
	   return schema
	else:
	   return self.schemaDict[tableName]

    # dump content of particular DB
    def printDBContent(self,table):
	"""Print content of given table in EventStore DB"""
        # get column names from the tables
        columnNames=[]
        if self.dbType=="sqlite":
	   query = "SELECT sql FROM sqlite_master WHERE name='%s'"%table
	   tup   = self.fetchOne(query)
	   schema= tup[0]
	   # SQLite prints schema in the following format:
	   # CREATE TABLE Name ( field1, type1, field2, type2 )
	   # we split string in such a way to extract fields
	   list  = string.split(schema,"(",1)[1:]
	   cList = string.split(list[0],",")
	   for idx in xrange(0,len(cList)):
	       cName = string.split(cList[idx])[0]
	       if cName!="PRIMARY":
	          columnNames.append(cName)
	else:
	   query = "DESCRIBE %s"%table
	   tup   = self.fetchAll(query)
	   for item in tup:
	       # first element in item is column name
	       cName = item[0]
	       columnNames.append(cName) 
	contentList = columnNames
	query = "SELECT * FROM "+table
	tup = self.fetchAll(query)
	print
	print "%s content:"%table
	emptyString="	"
	# get sizes of columns from contentList
	sizeList=[]
	for x in contentList:
	    sizeList.append(len(x))
	
	# store tuple content into finalList while counting largest
	# length of the object
	finalList=[]
	for x in tup:
	    for idx in xrange(0,len(x)):
		if len("%s"%x[idx])>len(contentList[idx]) and len("%s"%x[idx])>sizeList[idx]:
		   sizeList[idx]=len("%s"%x[idx])
	line=""
	line1=""
	for idx in xrange(0,len(sizeList)):
	    line+="="
	    line1+="-"
	    for i in xrange(0,sizeList[idx]): 
		line+="="
		line1+="-"
	print line
	for idx in xrange(0,len(contentList)):
	    fString=string.ljust(contentList[idx],sizeList[idx])
	    print fString,
	print
	print line1
	
	for x in tup:
	    for idx in xrange(0,len(x)):
		fString=string.ljust("%s"%x[idx],sizeList[idx])
		print fString,
	    print
	print line
	
    def makeESQuery(self,timeStamp=-1):
	"""Return the following dictionary: dict[time]=[(grade,minR,maxR,svName)]"""
	# first let's get all available grades
	query = "SELECT grade FROM Version WHERE state='active';"
#        print query
	tup = self.fetchAll(query)
	gradeList = []
	for x in tup:
	    elem = x[0]
	    if not gradeList.count(elem):
	       gradeList.append(elem)
	iQuery = """SELECT timeStamp,grade,minRunNumber,maxRunNumber,svName 
	FROM Version,GraphPath,SpecificVersion WHERE Version.graphid=GraphPath.graphid 
	AND GraphPath.svid=SpecificVersion.svid AND Version.state='active'"""
	if int(timeStamp)>=0:
	   iQuery+=" AND Version.timeStamp<='%s'"%timeStamp
	dict= {}
	for grade in gradeList:
	    query = iQuery+" AND Version.grade='%s' ORDER BY Version.timeStamp DESC, Version.minRunNumber ASC;"%grade
#            print query
	    tup = self.fetchAll(query)
	    for x in tup:
		time  = x[0]
		grade = x[1]
		minR  = x[2]
		maxR  = x[3]
		svName= x[4]
		if dict.has_key(time):
		   list = dict[time]+[(grade,minR,maxR,svName)]
		   dict[time]=list
		else:
#                   if len(dict.keys()) and int(timeStamp)>=0: continue
		   dict[time]=[(grade,minR,maxR,svName)]
	return dict
    # dump EventStore content in user readable format
    def printESInfo(self,timeStamp=-1):
	"""Dump EventStore DB content in user readable format."""
        dict = self.makeESQuery(timeStamp)
	keyList = dict.keys()
	keyList.sort()
	print "Requested timeStamp:",timeStamp 
	print "--------------------------------"
	print "   time  "
	print "          grade "
	print "          minRun maxRun details "
	print "--------------------------------"
	for key in keyList:
	    list  = dict[key]
	    print
	    if key=='0' or key==0: 
	       key = '00000000'
	    print key,
	    usedGrade = ""
	    usedSVName= ""
	    for item in list:
		grade = item[0]
		minR  = item[1]
		maxR  = item[2]
		if minR==1: minR = '000001'
		if maxR==1: maxR = '000001'
		svName= item[3]
		if usedGrade==grade:
		   output = "          %s %s"%(minR,maxR)
		else:
		   usedGrade =grade
		   output = "\n"
		   output+="          %s\n"%grade
		   output+="          %s %s"%(minR,maxR)
		if usedSVName!=svName:
		   output+=" %s"%svName
		   usedSVName = svName
		print output
		
    def printRuns(self,minR,maxR):
	"""Print list of runs for given run range"""
	query = "SELECT run FROM RunUID WHERE run>=%s and run<=%s;"%(minR,maxR)
	tup = self.fetchAll(query)
        print "For run range [%s,%s] the following runs found:"%(minR,maxR)
	for x in tup:
	    print "%d"%x[0]
	    
    def findFileForRun(self,run,time=0):
	"""Find location of the data file holding given run"""
        graphid  = 0
        if time:
           query = "SELECT graphid FROM Version WHERE timeStamp=%s AND maxRunNumber>=%s AND minRunNumber<=%s"%(time,run,run)
           tup   = self.fetchOne(query)
           if tup and tup[0]:
              graphid = tup[0]

	query = "SELECT locationFileId FROM Location WHERE run=%s"%(run)
        if graphid:
            query+=" AND graphid=%s"%graphid
        elif not graphid and time:
           print "No matched timeStamp found, continue searching in all graphs."
	tup = self.fetchAll(query)
        print "For given run %s"%(run,)
	if not len(tup):
	   print "No files found"
	   return
	for x in tup:
	    locId = x[0]
	    query = "SELECT fileName FROM FileID WHERE fileId=%s"%locId
	    res   = self.fetchOne(query)
            locFileName = res[0]
	    # locInfo=[streamNames,pdsIDList,oDict,recordSize,positionOfFirstRecord]
	    locInfo = lpds_dump.locationFileParser(locFileName)
            for pdsId in locInfo[1]:
	        query = "SELECT fileName FROM FileID WHERE fileId=%s"%pdsId
		result= self.fetchOne(query)
                print result[0]
            
    def getAllParents(self,childName):
	"""Get all parents for given child name"""
	graph = "digraph G {"
	query = "SELECT svid FROM SpecificVersion WHERE svName='%s';"%(childName,)
	tup   = self.fetchOne(query)
	dList = [childName]
	idList= []
	dict  = {}
	dictId= {}
	if tup and tup[0]:
	   svid=int(tup[0])
	   idList.append(svid)
	   dict[svid]=childName
	   while 1:
	      try:
	         svid=idList[0]
	      except:
	         break
	      query = """SELECT svName,svid FROM SpecificVersion,PathDepend WHERE
	                 childId='%s' AND parentId=svid"""%svid
	      tup = self.fetchAll(query)
	      parentList = []
	      for item in tup:
	         name = item[0]
		 id   = int(item[1])
		 dict[id]=name
	         if not dList.count(name): dList.append(name)
	         if svid==id: # something wrong, e.g. parentId=childId
		    print "ERROR: while lookup PathDepend with query"
		    print query
		    print "found parentId('%s')=childId('%s')"%(svid,id)
		    break
	         idList.append(id)
		 parentList.append(id)
	         graph+="""\n"%s"->"%s";"""%(dict[svid],name)
	      try:
	         dictId[svid]=parentList
	         idList.remove(svid)
	      except:
	         break
	graph+="\n}\n"
	return dList,idList,dict,dictId,graph
    def showDepend(self,childName):
        """Print all dependencies for given data version name."""
	dList,idList,dict,dictId,graph=self.getAllParents(childName)
#        print dict
#        print dictId
	if dList:
	   print "\nFor '%s' we found the following versions:"%childName
	   space = ""
           for item in dList:
	       print item
#               if not len(space):
#                  print "%s %s"%(space,item)
#               else:
#                  print "%s |-> %s"%(space,item)
#               space="  "+space
	else:
	   print "No such data version found",childName
	return


#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004, 2005
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""Set of helper classes to initialize ES toolkit"""

import os, sys, string, time, glob, re, stat, socket
import MySQLdb
import sqlite3 as sqlite
#import sqlite # pysqlite version 1
#import pysqlite2.dbapi2 as sqlite # pysqlite2
import esdb_auth, os_path_util, gen_util
from sql_util import *

class ESFileTypes:
    """Base class which defines file types accepted by EventStore"""
    def __init__(self):
        """Initialize file types supported in EventStore. Right now all of them are
	hard-coded: evio, hddm, ikey.   Among them a separate list of
	files which allowed to be injected: evio, hddm, idxa."""
	#self.ESFileTypes = ["hddm","ikey","lhddm"]
	self.ESFileTypes = ["evio","hddm","rest","mc","ikey"]
	self.injectionFileTypes = ["evio","hddm","rest","mc","idxa"]
	#self.locFileTypes = ["lhddm"]
	self.keyFileTypes = ["ikey"]
	self.datFileTypes = ["evio","hddm,""rest","mc"]
	
    def isLocType(self,iFileType):
        """Check if given file type is location file.
        @type iFileType: string
        @param iFileType: file type
        @rtype: integer
        @return: non-zero value if given file type is one of the location types, e.g. lhddm
        """
        return self.locFileTypes.count(iFileType)

    def isKeyType(self,iFileType):
        """Check if given file type is index file.
        @type iFileType: string
        @param iFileType: file type
        @rtype: integer
        @return: non-zero value if given file type is the key file type
        """
        return self.keyFileTypes.count(iFileType)

    def isDatType(self,iFileType):
        """Check if given file type is data file.
        @type iFileType: string
        @param iFileType: file type
        @rtype: integer
        @return: non-zero value if given file type is one of the data types, e.g. hddm
        """
        return self.datFileTypes.count(iFileType)

    def esFileTypes(self):
        """Return a list of known file types supported in EventStore.
        @rtype: list
        @return: list of known file types supported in EventStore
        """
        return self.ESFileTypes
	
    def esInjectionFileTypes(self):
        """Return a list of known file types supported in EventStore which allowed
	for injection.
        @rtype: list
        @return: list of known file types supported for injection to EventStore
        """
        return self.injectionFileTypes
	
    def allow(self,type):
        """Check if given file type is allowed in EventStore. 
        @rtype: integer
        @return: true or false.
        """
        if self.ESFileTypes.count(type):
	   return 1
	return 0
	
    def allowToInject(self,type):
        """Check if given file type is allowed for injection in EventStore. 
        @rtype: integer
        @return: true or false.
        """
        if self.injectionFileTypes.count(type):
	   return 1
	return 0
	
class ESInit(ESFileTypes,SQLUtil):
    """Base class which establish connection with EventStore"""
    def __init__(self,db, dbType, logFile):
        """Initialize database pointer, cursor, db type (MySQL or SQLite). Retrieve
	information about table names from underlying DB."""
	ESFileTypes.__init__(self)
	SQLUtil.__init__(self,db,dbType,logFile)
	self.error= 0
	self.ok   = 1

def helpMsg(tool,localOpt):
    """Form usage message. Defines a list of common options for every tool used
    in EventStore toolkit. Among them '-help', '-esdb', etc.
    @type localOpt: list
    @param localOpt: list of options
    @rtype: string
    @return: help message how to use toolkit
    """
    initMsg = "\nUsage: %s "%tool
    offset  = "\n"
    for i in xrange(1,len(initMsg)):
        offset+=" "
    msg = initMsg+"[ -help ] [ --help ] [ -examples ] [ -profile ]"
    msg+= offset +"[ -verbose ] [ -historyfile <filename> ]"
    msg+= offset +"[ -db <name@host:port:socket or fileName> ]"
    msg+= offset +"[ -user <username> -password <password> ] ]"
    msg+= offset +"[ -logFile </path/filename or 'stdout' or 'stderr'> ]"
    for item in localOpt:
        msg+=offset+item
    msg+= "\n"
    msg+= "\nUse ESDB environment variable to setup your DB"
    msg+= "\n     Example: ESDB=EventStoreTMP@hallddb"
    msg+= "\n     Example: ESDB=/tmp/sqlite.db"
    msg+= "\nUser name and password for MySQL access can be stored into $HOME/.esdb.conf as"
    msg+= "\n<username>:<password>"
    msg+= "\n"
    msg+= "\nOptions can be specified in any order."
    msg+= "\nFor option description please run '%s --help'"%tool
    msg+= "\nFor use cases please run '%s -examples'"%tool
    msg+= "\nContact: Sean Dobbs, s-dobbs@northwestern.edu\n"
    return msg

def checkArg(optValues):
    """Check provided list of arguments for leading '-'.
    @type optValues: list
    @param optValues: list of argument values provided by user
    @rtype: none
    @return: exit 1 if given value is not allowed, otherwise return nothing.
    """
    sys.__stdout__.flush()
    for val in optValues:
        value = "%s"%val
	if value[0]=="-":
	   print "Found argument which starts with '-':",value
	   sys.__stdout__.flush()
	   sys.exit(1)
	
def checkPythonVersion(version):
    """Check if provided version is greater then current version of python.
    @type version: string
    @param version: python version, e.g. 2.4
    @return: true or false
    """
    pythonVersion = sys.version
    currentVersion= float(pythonVersion[0]+"."+pythonVersion[2])
    if currentVersion>=float(version):
       return 1
    return 0

def decodeHostNameString(hostName):
    """Decode provided string to dbName@dbHost:dbPort:dbSocket
    """
    tup    = string.split(hostName,"@")
    dbPort = ""
    dbSock = ""
    if len(tup)==2:
       dbName= tup[0]
       dbHost= tup[1]
    else:
       dbName="EventStoreTest"
       dbHost=tup[0]
    ntup   = string.split(dbHost,":")
    if len(ntup)==2:
       dbHost = ntup[0]
       dbPort = ntup[1]
    if len(ntup)==3:
       dbHost = ntup[0]
       dbPort = ntup[1]
       dbSock = ntup[2]
    if dbPort:
       # check that port consist only from numbers
       if re.search("[a-zA-Z]",dbPort):
          print "You provided",hostName
          raise "Please specify correct name, in form of dbName@dbHost:<dbPort>:<dbSocket>"
       return (dbName,dbHost,int(dbPort),dbSock)
    return (dbName,dbHost,dbPort,dbSock)

def ESDBConnector(dbHost,dbName,userName="",userPass="",isolationLevel="",dbPort="",dbSocket=""):
    """ESDBConnector connect to given host(MySQL) and/or file(SQLite) using dbName.
    In the cae of SQLite user may pass isolationLevel:
       - default mode, if no isolationLevel is passed will start db with BEGIN
       - autocommit mode, isolationLeve='None'
       - you may also pass: DEFERRED, IMMEDIATE or EXCLUSIVE levels
         - DEFERRED deffer locks until first access
	 - IMMEDIATE implies lock for first BEGIN IMMEDIATE and doesn't allow
	   anything to write to DB, but allow to read
         - EXCLUSIVE will lock entire DB.
    We use IMMEDIATE for SQLite and READ COMMITTED isolation level for MySQL.
    We also turn off autocommit.
    @type dbHost: string
    @type dbName: string
    @type userName: string
    @type userPass: string
    @type isolationLevel: string
    @type dbPort: integer
    @type dbSocket: string
    @param dbHost: name of the host, lnx151.lns.cornell.edu or db file name /sqlite.db
    @param dbName: DB name, obsolete in the case of SQLite
    @param userName: user name
    @param userPass: password
    @param isolationLevel: valid only for SQLite, setup isolation level
    @param dbPort: port number, e.g. default for MysQL is 3306
    @param dbSocket: socket name, e.g. /var/log/mysql=
    @rtype: tuple (db, dbType)
    @return: return object to underlying DB and its type, e.g. "sqlite" or "mysql"
    """
    sqLite = 0
    try:
       ipAddress = socket.gethostbyname(dbHost)
    except:
       # if name resolution failed we assume that our dbHost is flat file name
       curDir  = os.path.split(dbHost)[0]
       if not curDir: curDir=os.getcwd()
       dirList = os.listdir(curDir)
       if os.path.isfile(dbHost) or not dirList.count(dbHost):
	  sqLite=1
       else:
          print "Cannot resolve dbHost='%s'"%dbHost
	  sys.exit(1)
    # read userName and password from user-specific file
    if not sqLite:
       # check login name and password to access EventStore MySQL DB.
       userName, userPass = esdb_auth.authToESMySQL(dbHost,userName,userPass)
    # connect to MySQL EventStoreDB
    if sqLite:
       if isolationLevel:
	  db = sqlite.connect(dbHost,timeout=15.0,isolation_level=isolationLevel)
       else:
	  db = sqlite.connect(dbHost,timeout=15.0,isolation_level=None)
       dbType = "sqlite"
    else:
       if not dbName and dbHost:
          print "You are attempting to connect to host '%s'"%dbHost
          print "But did not specified DB name '%s'"%dbName
          sys.exit(1)
       try:
          if dbPort:
	     db = MySQLdb.connect(host=dbHost,user=userName,passwd=userPass,db=dbName,port=int(dbPort))
          else:
	     db = MySQLdb.connect(host=dbHost,user=userName,passwd=userPass,db=dbName)
       except:
	  print "WARNING: First attempt to connect to %s@%s:%s:%s fail, try again"%(dbName,dbHost,dbPort,dbSocket)
	  time.sleep(60)
	  try:
             if dbPort:
               db = MySQLdb.connect(host=dbHost,user=userName,passwd=userPass,db=dbName,port=int(dbPort))
             else:
               db = MySQLdb.connect(host=dbHost,user=userName,passwd=userPass,db=dbName)
	  except:
	     print "ERROR: Second attempt to connect to %s@%s:%s:%s fail, please investigate"%(dbName,dbHost,dbPort,dbSocket)
	     gen_util.printExcept()
	     sys.exit(1)
       dbType = "mysql"
       if userName!="eventstore":   ## master DB user name
          cursor = db.cursor()
#           cursor.execute("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE")
          cursor.execute("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED")
          cursor.execute("SET AUTOCOMMIT=0")
          cursor.close()
    return (db,dbType)

def connectToMasterDB(dbName,dbHost,port,socket,verbose=0):
    """Connect to master DB
    @type dbName: string
    @param dbName: name of underlying DB, e.g. EventStore
    @type dbHost: string
    @param dbHost: hostname or file name for DB, e.g. lnx151 or sqlite.db
    @type port: integer
    @param port: port number, e.g. 3306 is default for MySQL
    @type socket: string
    @param socket: file name of the socket
    @rtype: tuple
    @return: db pointer and db type, e.g. "sqlite"
    """
    masterTuple = ()
    if not dbName and not dbHost and not port and not socket:
       login,adminInfo,dbName,dbHost,port,socket=esdb_auth.readConfigFile()
    msg = "Connect to master %s@%s"%(dbName,dbHost)
    if port: msg+=":%s"%port
    if socket: msg+=":%s"%socket
    if verbose:
       print msg
    if not dbHost:
       return masterTuple
    db,dbType=ESDBConnector(dbHost,dbName,'eventstore','',"",port,socket)
    return db,dbType

def requestDataFromDB(dbName,dbHost,port,socket,query,whatToRetrieve="all",verbose=1):
    """Send query to specified DB. Return tuple for given query.
    @type dbName: string
    @param dbName: name of underlying DB, e.g. EventStore
    @type dbHost: string
    @param dbHost: hostname or file name for DB, e.g. lnx151 or sqlite.db
    @type port: integer
    @param port: port number, e.g. 3306 is default for MySQL
    @type socket: string
    @param socket: file name of the socket
    @type query: string
    @param query: SQL query
    @type whatToRetrieve: string
    @param whatToRetrieve: how to retrieve query, a single row or all matched rows
    @type verbose: integer (default is yes)
    @param verbose: verbosity flag
    @rtype: tuple
    @return: result's tuple associated with requested query
    """
    masterTuple = ()
    if not dbName and not dbHost and not port and not socket:
       login,adminInfo,dbName,dbHost,port,socket=esdb_auth.readConfigFile()
    msg = "Connect to master %s@%s"%(dbName,dbHost)
    if port: msg+=":%s"%port
    if socket: msg+=":%s"%socket
    if verbose:
       print msg
    if not dbHost:
       return masterTuple
    db,dbType=ESDBConnector(dbHost,dbName,'eventstore','',"",port,socket)
    cu = db.cursor()
    # send request to DB
    cu.execute(query)
    if whatToRetrieve=="all":
       masterTuple = cu.fetchall()
    else:
       masterTuple = cu.fetchone()
    return masterTuple

def ESOptions(userCommand,optList,usage="",usageDescription="",examples=""):
    """Analyse common options of EventStore toolkit:
    [ -help ] [ --help ] [ -examples ] [ -profile ]
    [ -verbose ] [ -historyfile <filename> ]
    [ -db <name@host:port:socket or fileName> ]
    [ -user <username> -password <password> ]
    [ -logFile </path/filename or 'stdout' or 'stderr'> ]
    @rtype: tuple (oList,dictOpt)
    @return: oList=[dbName,dbHost,userName,userPass,dbPort,dbSocket,histFile,logFile,logDir,
    verbose,profile,userCommand]
    dictOpt[option]=value
    """
    #print len(optList)
    #if len(optList) == 1 or optList[1] == "-help":
    if len(optList) == 1:
       print usage
       sys.exit()
    # default values
    verbose    = 0
    profile    = 0
    # read first from environment ESDB
    db         = "sqlite.db"
    dbHost     = "sqlite.db"
    dbPort     = "" #3306 # default MySQL
    dbSocket   = "" #"/var/lib/mysql/mysql.sock" # default MySQL
    dbName     = "EventStoreTMP"
    try:
       esdb = os.environ['ESDB']
       dbName,dbHost,dbPort,dbSocket = esdb_auth.dissassembleDBName(esdb) 
       db   = dbName+"@"+dbHost+":"+dbPort+":"+dbSocket
    except:
       pass

    logFile    = os.path.join(os.getcwd(),"esdb.log")
    histFile   = os.path.join(os.getcwd(),"esdb.history")
    userName   = "" # user name to login to MySQL
    userPass   = "" # password to login to MySQL
    userCommand+=" "
    dictOpt    = {}
    x = 1
    # form user's command
    doNotRead  = 0
    while x < len(optList):
      if optList[x] == "--help" or optList[x] == "-help":
         dictOpt[optList[x]]=1
	 print usageDescription
	 sys.exit()
      if optList[x] == "-examples" or optList[x]=="--examples":
         dictOpt[optList[x]]=1
	 if examples:
	    print examples
	 else:
	    print ESExamples()
	 sys.exit()
      try:
	 if doNotRead:
	    userCommand+="XXXXXX "
	    doNotRead   = 0
	 else:
	    userCommand+=optList[x]+" "
	 if optList[x] == "-db": 
            dictOpt[optList[x]]=2
	    db = optList[x+1]
	 if optList[x] == "-historyFile" or optList[x] == "-historyfile": 
            dictOpt[optList[x]]=2
	    histFile = optList[x+1]
	    if histFile[0]!="/":
	       print "ERROR: absolute file name is required for %s option, e.g. %s /path/myhist.txt"%(optList[x],optList[x])
	       raise
	 if optList[x] == "-logFile" or optList[x] == "-logfile" : 
            dictOpt[optList[x]]=2
	    logFile = optList[x+1]
	    if logFile[0]!="/" and logFile!="stdout" and logFile!="stderr":
	       print "ERROR: absolute file name is required for %s option, e.g. %s /path/my.log"%(optList[x],optList[x])
	       raise
	 if optList[x] == "-verbose": 
            dictOpt[optList[x]]=1
	    verbose= 1
	 if optList[x] == "-profile": 
            dictOpt[optList[x]]=1
	    profile= 1
	 if optList[x] == "-user": 
            dictOpt[optList[x]]=2
	    userName = optList[x+1] 
	 if optList[x] == "-password":
            dictOpt[optList[x]]=2
	    doNotRead   = 1
	    userPass = optList[x+1] 
	    if userPass[0]=="-":
	       print "Please verify password"
	       raise
	 x = x+1
      except:
	 print usage
	 sys.exit(1)
    userCommand+="\n"
    if verbose:
       print userCommand
    if not os.path.isdir(os.path.split(logFile)[0]) and logFile!='stdout':
       print "You requested to write log file to '%s'"%logFile
       print "but directory '%s' does not exists"%os.path.split(logFile)[0]
       sys.exit(1)
    # we need to parse db into dbName,dbHost,dbPort,dbSocket
    dbName,dbHost,dbPort,dbSocket=esdb_auth.dissassembleDBName(db)
    
    oList = [(dbName,dbHost,userName,userPass,dbPort,dbSocket)]
    oList+= [(histFile,logFile,verbose,profile)]
    oList.append(userCommand)
    return  (oList,dictOpt)
    
def ESOutputLog(logFile):
    """Setup EventStore db log. It is either stdout, stderr or file based.
    @type logFile: string
    @param logFile: name of the log file
    @rtype: tuple (outputLog,globalLog)
    @return: outputLog is a open file descriptor to the output log file
    globalLog is a open file descriptor for global EventStore log, e.g. multiple jobs
    can write to global log file and it's local own log file
    """
    localtime  = time.strftime("%Y%m%d_%H%M%S",time.localtime())
    tempLogFile    = "esdb.log.%s_%s"%(localtime,os.getpid())
    gLog = os.path.join(os.getcwd(),"esdb.log")
    if logFile=="stdout":
       outputLog=sys.stdout
       globalLog=open(gLog,"a")
    elif logFile=="stderr":
       outputLog=sys.stderr
       globalLog=open(gLog,"a")
    else:
       gDir = os.path.split(logFile)[0]
       outputLog = open(os.path.join(gDir,tempLogFile),"w")
       globalLog = open(logFile,"a")
    return (outputLog,globalLog)

def ESInput(userCommand,outputLog,dbType):
    """Write userCommand information into EventStore db. userCommand is a command with
    their option invoked by user.
    @type userCommand: string
    @param userCommand: command invoked by user
    @type outputLog: file descriptor
    @param outputLog: open file descriptor
    @type dbType: string
    @param dbType: type of underlying DB, e.g. "sqlite" or "mysql"
    @rtype: none
    @return: none
    """
    localtime = "%s"%time.strftime("%H:%M:%S",time.localtime())
    pid       = "%s"%os.getpid()
    user      = os.environ["USER"]
    s = "\n\n%s %s ###### %s@%s\n"%(pid,localtime,user,os.uname()[1])	
    s+= "%s %s ###### %s\n"%(pid,localtime,userCommand)
    outputLog.write(s)
    s = "%s started process %s at %s\n"%(os.uname()[1],os.getpid(),time.asctime())	
    localtime = "%s"%time.strftime("%H:%M:%S",time.localtime())
    outputLog.write("%s %s ###### %s initialization is completed\n"%(pid,localtime,dbType))

def ESOutput(status,userCommand,historyFile,outputLog,globalLog):
    """Write out final information about job status to EventStore db log.
    @type status: integer
    @param status: status code of injection
    @type userCommand: string
    @param userCommand: command invoked by user
    @type historyFile: string
    @param historyFile: file name
    @type outputLog: file descriptor
    @param outputLog: open file descriptor
    @type globalLog: file descriptor
    @param globalLog: open file descriptor
    @rtype: integer
    @return: status code
    """
    # check status, if we successfully updated DB commit transaction
    if status==1:
       # update user's command history
       comFile = open(historyFile,'a')
       comFile.write(userCommand)
       comFile.close()
    else:
       print "Error condition triggered, job terminated"

    returnStatus = 0
    if status==1: # everything is fine
       msg = "Job completed"
    else:
       msg = "Job aborted, see %s for details"%outputLog.name
       returnStatus = -1
    pid       = "%s"%os.getpid()
    localtime = "%s"%time.strftime("%H:%M:%S",time.localtime())
    outputLog.write('\n%s %s ###### %s\n'%(pid,localtime,msg))
    logName = outputLog.name
    outputLog.close()
    if logName!='<stdout>' and logName!='<stderr>':
       logInfo = open(logName,'r').read()
       if globalLog:
	  globalLog.write(logInfo)
	  globalLog.close()
       if status==1:
	  os.remove(logName)
    return returnStatus	

# examples how to add and manage data in EventStore  ### CHECK
def ESExamples():
    """Contain a list of usefull examples how to add/modify data to EventStore.
    @rtype: none
    @return: none
    """
    examples = """
List of common EventStore injection tasks, for more option use -help/--help options:

Adding all files from /work/halld/tem/myData directory into physics grade using 20090215
timeStamp and recon-data99_vs1 data version name. The key files will be written
into the /work/halld/tem/myData directory. All data will be injected on MySQL running
on hallddb into EventStoreTMP (which is default DB name):
ESBuilder -add /work/halld/tem/myData -grade physics -time 20090215 
          -dataVersionName recon-data99_vs1 -mysql hallddb

Adding pattern (My*.hddm) from /work/halld/tem/myData directory into physics grade using 20090215
timeStamp, recon-data99_vs1 data version name and qcd view. Put output key files
into /work/halld/tem/output. At this time we inject into sqlite.db
ESBuilder -add /work/halld/tem/myData/My*.pds -grade physics -time 20090215 
          -dataVersionName P2-data99_vs1 -output /work/halld/tem/output -view qcd
          -sqlite /work/halld/tem/sqlite.db

Injection of raw data:
ESBuilder -mysql lnxXXX -output /work/halld/tem/index -time 0 -grade rawdata
          -dataVersionName rawdata_RunPeriod-201X-ZZ -add /work/halld/rawdata/hd_rawdata_002517_03d.evio

Add file /work/halld/tem/myData/b1piTag.hddm into physics grade using 20090215
timeStamp. Associate this data with recon-data99_vs1 (parent graph) and assign
P2-data99-DTag data version name. Put output key files
into /work/halld/tem/output. Here we also specify a concrete DB we going to use, EventStoreDB.
ESBuilder -add /work/halld/tem/myData/b1piTag.hddm -grade physics -time 20090215 
          -dataVersionName recon-data99-b1piTag -listOfParents recon-data99_vs1    
	  -output /work/halld/tem/output -mysql halldb -esdb EventStoreDB
    """
    return examples
	

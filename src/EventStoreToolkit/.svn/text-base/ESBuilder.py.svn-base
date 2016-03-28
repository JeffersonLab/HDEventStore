#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
"""
EventStore builder supports SQLite/MySQL DBs through sqlite and MySQLdb 
modules, respectively.
All available options are declare below and can be viewed by using 
-help option.
It keep track of users command through esdb.history file
Log of all SQL queries are saved into esdb.log
Compensation SQL queries can be found in esdb.compensate_YYYYMMDD_HHMMSS_PID
"""

import os, sys, string, time, glob, stat
import ESManager, os_path_util, es_init, sql_util, gen_util, esdb_auth
from es_init import ESInit, checkArg


def ESBuilder(args):
    """ESBuilder is a main injection tool. It supports two types of DBs:
    MySQL and SQLite. The injection can be done for variety of file formats:
    evio, hddm, idxa. For option information and usage please use '-help' option.
    For option description '--help'.
    For specific injection types please use '-examples' option.

    Please note, ESBuilder is a wrapper shell script around ESBuilder.py
    module which does the work.
    """
    localOpt =["[ -add <dir or file or pattern of files> ]"]
    localOpt.append("[ -grade <grade> ] [ -time <timeStamp> ]")
    localOpt.append("[ -dataVersionName <name> ]  [ -view <skim> ]")
    localOpt.append("[ -listOfParents <dataVersionName's> ]")
    localOpt.append("[ -output <dir> ] [ -HSMDir <HSM directory> ]")
    localOpt.append("[ -dupRead <fileName> ] [ -skim ] [ -no-skim ]")
    localOpt.append("[ -masterDB <name@host:port:socket or fileName> ]")
    usage=es_init.helpMsg("ESBuilder",localOpt)
    usageDescription="""
Option description:
*   -grade:   specifies the grade, e.g. "physics", "p2-unchecked"
*   -add:     adds data file(s) to the EventStore
	      You may specify: directory, file name or a list of files
	      For patterns use '*', e.g MC*tau*.pds
*   -output:  output location for storing key/location files
*   -dataVersionName: specifies the data version name (aka svName)

    -time:    specifies the timeStamp, e.g. 20090227. If time is not provided 
              will try to append to existing grade/dataVersionName or use a 
	      one day in a future as new timeStamp if no grade/dataVersionName
	      combination is found.
    -view:    specifies the view, e.g. "tau"
    -listOfParents specifies list of parents for given injection,
	      e.g. while injecting p2-unchecked grade its parent is 'daq'.
    -newDB:   force the creation of a new EventStore
    -sqlite   use the SQLite version of EventStore
	      default sqlite.db, otherwise a fileName needs to be provided
    -mysql    use the MySQL version of EventStore. In order to access MySQL
	      you need either provide login/password through the -user/-password
	      options or create $HOME/.esdb.conf with user:password entry
    -masterDB specifies host and db name of the master you want to use
    -verbose: verbose mode, a lot of useful printout
    -idleMode when this flag is specified, no key/location file will be
	      generated (useful once you have them and want reproduce DB
	      content). But content of DB will be updated. USE WITH CAUTION.
    -delete   delete a grade from EventStore. USE WITH CAUTION.
	      You need to provide the grade and the timeStamp.
    -HSMDir   specifies output HSM directory.
    -logFile  specifies the log file name. You may either provide a full file name 
              (including path) or 'stdout' or 'stderr' to redirect your log to
	      appropriate I/O stream. During injection an intermidiate files
	      esdb.log.YYYYMMDD_HHMMSS_PID will be created.
	      Once a job successfully finishes, the esdb.log.YYYYMMDD_HHMMSS_PID 
	      is moved to your logFile, otherwise esdb.log.YYYYMMDD_HHMMSS_PID remains.
    -profile  perform internal profiling.
    -dupRead  in the case of duplicated records force to use this source
    -skim     force ESBuilder to use input files as a skim, i.e. find their parents
              and build combined location file for all of them
    -no-skim  force ESBuilder to use input files as is

Please note: required parameters are marked with (*). All options can be
specified in any order. By default: view='all', EventStoreTMP DB is used and key/location
files are generated.

    """

    examples   = es_init.ESExamples()
    userCommand="ESBuilder.py"
    optList, dictOpt = es_init.ESOptions(userCommand,args,usage,usageDescription)
    dbName,dbHost,userName,userPass,dbPort,dbSocket = optList[0]
    historyFile,logFile,verbose,profile             = optList[1]
    userCommand                                     = optList[2]

    # default values
    grade         = ""
    timeS         = gen_util.dayAhead()
    oDir          = ""
    view          = "all"
    run           = 0
    file          = ""
    newDB         = 0
    delete        = 0
    genMode       = 1
    minRun        = 0
    maxRun        = 1000000
    localtime     = time.strftime("%Y%m%d_%H%M%S",time.localtime())
    uname         = os.uname()	
    svName        = ""
    tempLogFile   = "esdb.log.%s_%s"%(localtime,os.getpid())
    fileList      = []
    listOfParents = []
    oHSMDir       = ""
    dupRead       = ""
    skim          = 0
    noskim        = 0
    masterDBName  = dbName
    masterDBHost  = dbHost
    master        = ""
    masterDB      = ""
    masterDBPort  = dbPort
    masterDBSocket= dbSocket
    # parse the rest of the options and form user's command
    x = 1
    doNotRead  = 0
    while x < len(args):
      try:
	 if args[x] == "-newDB": 
	    newDB  = 1
	    x+=1
	    continue
	 if args[x] == "-HSMDir": 
	    oHSMDir = args[x+1]
	    checkArg([oHSMDir])
	    x+=2
	    continue
	 if args[x] == "-dupRead": 
	    dupRead = args[x+1]
	    checkArg([dupRead])
	    x+=2
	    continue
	 if args[x] == "-dataVersionName": 
	    svName = args[x+1]
	    checkArg([svName])
	    x+=2
	    continue
	 if args[x] == "-grade": 
	    grade  = args[x+1]
	    checkArg([grade])
	    x+=2
	    continue
	 if args[x] == "-time": 
	    timeS  = args[x+1]
	    checkArg([timeS])
	    x+=2
	    continue
	 if args[x] == "-output": 
	    oDir   = args[x+1]+"/"
	    checkArg([oDir])
	    x+=2
	    continue
	 if args[x] == "-runRange": 
	    minRun=int(args[x+1])
	    maxRun=int(args[x+2])
	    checkArg([minRun,maxRun])
	    x+=3
	    continue
	 if args[x] == "-listOfParents": 
	    x+=1
	    while(args[x][0]!="-"):
	       newArg = args[x]
	       listOfParents.append(args[x])
	       x+=1
	       if len(args)==x:
		  break
	    checkArg(listOfParents)
	    continue
	 if args[x] == "-add": 
	    file = os_path_util.formAbsolutePath(args[x+1])
	    # first check if pattern is present
	    if len(args)>x+2 and args[x+2][0]!="-":
	       counter=0
	       for idx in xrange(x+1,len(args)):
		   newArg  = args[idx]
		   if newArg[0]=="-":
		      break
		   counter+=1
		   if os.path.isfile(newArg):
		      fileList.append(os_path_util.formAbsolutePath(newArg))
	       x+=counter+1
	       continue
	    elif os.path.isdir(file):
	       dir = file+"/"
	       for f in os.listdir(dir):
		   if string.split(f,".")[-1]!="pds": continue
		   fileName=dir+f
		   fileList.append(os_path_util.formAbsolutePath(fileName))
	       x+=2
	       continue
	    elif os_path_util.isFile(file):
	       if file[-5:]==".list":
		  tmpList = open(file).readlines()
		  for item in tmpList:
		      fileList.append(string.split(item)[0])
	       else:
		  fileList = [file]
	       x+=2
	       continue
	    # check if this file exists
	    else:
	       print "ESBuilder: no such file",file
	       raise
	    checkArg(fileList)
	 if args[x] == "-view":
	    view = args[x+1]
	    checkArg([view])
	    x+=2
	    continue
	 if args[x] == "-idleMode": 
	    genMode= 0
	    x+=1
	    continue
	 if args[x] == "-skim": 
	    skim= 1
	    x+=1
	    continue
	 if args[x] == "-no-skim": 
	    noskim= 1
	    x+=1
	    continue
	 if args[x] == "-masterDB": 
	    masterDB = args[x+1]
            master   = 1
	    checkArg([masterDB])
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

    ### AUTHENTICATION???
    # check that USER=pass2, otherwise exit
    authUsers = ['gluex','sdobbs']   ### CHECK
    # check if USER environment is set up, otherwise use LOGNAME
    env = os.environ
    if not env.has_key('USER'):
       os.environ['USER']=env['LOGNAME']
    if not authUsers.count(os.environ["USER"]) and dbName=="EventStore" and string.find(dbHost,'hallddb')!=-1:
       print "ERROR: Injection to db='EventStore' should be done from official (gluex) account for %s DB\n"%dbName
       print "For your own injection please use another db name"
       sys.exit(1)
    # check underlying OS, so far we only allow to inject from SunOS
    #if os.environ["USER"]=="pass2" and uname[0]!="SunOS":
    #   print "ERROR: for pass2 account the EventStore injection should be done from SunOS\n"
    #   sys.exit(1)
    #######################################

    # form normalized abosulte paths
    oDir=os_path_util.formAbsolutePath(oDir)
    
    # check required parameters
    if not len(grade):
       print "ESBuilder requires to specify a grade, see -grade option"
       sys.exit(1)
    if string.find(grade,"unchecked")==-1 and view=="all":          ### CHECK
       print "ESBuilder only allow to inject 'unchecked' grades"
       print "  daq-unechecked, p2-unchecked, physics-unchecked" 
       print "Either specify different view or inject as unchecked grade"
       print "Given grade='%s' view='%s'"%(grade,view)
       sys.exit(1)
    if not len(fileList):
       print "ESBuilder requires to specify input file(s) with -add option"
       sys.exit(1)
    
    # check permissions and access to output dir
    if not os.path.isdir(oDir):
       print "Output directory '%s' doesn't exists"%oDir
       print "ESBuilder requires to specify output dir to store key/location files, see -output option"
       sys.exit(1)
    if oDir and not os_path_util.checkPermission(oDir):
       print "You don't have permission to write to output area '%s'"%oDir
       sys.exit(1)
    
    # check permission to write to HSM
    if oHSMDir and not os.path.isdir(oHSMDir):
       print "HSM directory '%s' does not exists"%oHSMDir
       sys.exit(1)
       if not os_path_util.checkPermission(oHSMDir):
          print "You don't have permission to write to HSM location '%s'"%oHSMDir
          sys.exit(1)
       
    # check that all inputs are in place
    for file in fileList:
        if not os.path.isfile(file):
           print "File '%s' does not exists"%file
           sys.exit(1)
    if dupRead and not os.path.isfile(dupRead):
       print "File '%s' does not exists"%dupRead
       sys.exit(1)
    
    # connect to MySQL EventStoreDB
    outputLog, globalLog = es_init.ESOutputLog(logFile) 
    db, dbType           = es_init.ESDBConnector(dbHost,dbName,userName,userPass,'',dbPort,dbSocket)
    es_init.ESInput(userCommand,outputLog,dbType)

    # Be verbose
    dbinfo="\t grade\t'%s'\n\t timeStamp\t'%s'\n\t view\t\t'%s'\n"%(grade,timeS,view)
    if newDB:
       if verbose:
	  print "Creating new tables DB:"
	  print dbinfo
    else:
       if verbose:
	  print "Updating existing tables in DB:"
	  print dbinfo
    if genMode==0 and verbose: 
       print "\n\t ===> Process running in Idle mode"

    # create instance of ESManager class
    mydb = ESManager.ESManager(db,dbType,outputLog)
    # set-up all parameters
    mydb.setOutputDir(oDir)
    mydb.setGenerateDB(newDB)
    mydb.setSVName(svName)
    mydb.setParents(listOfParents)
    mydb.setGrade(grade)
    mydb.setTimeStamp(timeS)
    mydb.setView(view)
    mydb.setMinRun(minRun)
    mydb.setMaxRun(maxRun)
    mydb.setVerboseLevel(verbose)
    mydb.setReadDuplicatesSource(dupRead)
    mydb.setSkimFlag(skim)
    mydb.setNoSkimFlag(noskim)
    mydb.setDBHost(dbHost)
    mydb.setDBName(dbName)
    mydb.setDBPort(dbPort)
    mydb.setDBSocket(dbSocket)

    # interpret the master option
    if  masterDB:
        dbComponents = string.split(masterDB,"@")
        if len(dbComponents)==2:
           masterDBName = dbComponents[0]
           newComponents= string.split(dbComponents[1],":")
           masterDBHost = newComponents[0]
           port = socket = ""
           if len(newComponents)==2:
              port  = newComponents[1]
           elif len(newComponents)==3:
              socket= newComponents[2]
#           masterDBHost,port,socket=string.split(dbComponents[1],":")
           if port:
              masterDBPort   = port
           if socket:
              masterDBSocket = socket
        else:
           masterDBHost = dbComponents[0]
    else:
      login,adminInfo,cMasterName,cMasterHost,cMasterPort,cMasterSocket=esdb_auth.readConfigFile()
      if cMasterHost:
         masterDBHost  = cMasterHost
         masterDBName  = cMasterName
         masterDBPort  = cMasterPort
         masterDBSocket= cMasterSocket
    mydb.setMasterDB(masterDBName,masterDBHost,masterDBPort,masterDBSocket)

    # update DB using transaction
    if delete:
       status = mydb.deleteGrade(delGrade,delTime)
    else:     # for anything else
       try:
          status = mydb.updateDB(genMode,fileList,oHSMDir)
       except:
          print "ERROR: fail to process:"
	  for item in fileList:
	      print item
	  print "--------------- See traceback ----------------"
	  raise

    # close connection to db
    mydb.commit()
    mydb.close()

    returnStatus = es_init.ESOutput(status,userCommand,historyFile,outputLog,globalLog)
    return returnStatus
    

#
# main
#
if __name__ == "__main__":
    if not es_init.checkPythonVersion("2.4"):
       print "To run ESBuilder you need to have 2.4 or higher version of python"
       sys.exit(1)
    if sys.argv.count('-profile') and es_init.checkPythonVersion("2.3"):
       # include python profiler only if python version greater then 2.3
       import hotshot			# Python profiler
       import hotshot.stats		# profiler statistics
       print "Run ESBuilder/ESManager in profiler mode"
       profiler = hotshot.Profile("profile.dat")
       profiler.run("ESBuilder(sys.argv)")
       profiler.close()
       stats = hotshot.stats.load("profile.dat")
       stats.sort_stats('time', 'calls')
       stats.print_stats()
    else:
       status = ESBuilder(sys.argv)
       sys.exit(status)

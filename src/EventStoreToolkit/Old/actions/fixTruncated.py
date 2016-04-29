#!/usr/bin/env python
"""A helper script to fix truncated run in EventStore. The original key file entry
is copied to view='all-problem', then new IDXA file is used to create a new key file.
Its entry is replaced in KeyFile table of EventStore DB.
"""

import os, sys, string, time, popen2
import MySQLdb, es_init
#import sqlite # pysqlite version 1
#import pysqlite2.dbapi2 as sqlite # pysqlite2
import sqlite3 as sqlite

#
# main
#
if __name__ == "__main__":
    usage="""
    ESFixTruncatedRun [-help] <inputIDXAFile> [<ESDBName=EventStoreTest>] [<gradeName=daq>]
    """
    # check for help option
    if len(sys.argv)==1 or sys.argv[1]=='-help' or sys.argv[1]=='--help':
       print usage
       sys.exit()
            
    # input file
    iFile  = sys.argv[1]
    if not os.path.isfile(iFile):
       print "File '%s' not found"%iFile
       sys.exit(1)

    dbName  = 'EventStoreTest'          ### UPDATE
    dbHost  = 'lnx151.lns.cornell.edu'  ### UPDATE
    dbPort  = 3307
    if len(sys.argv)>=3 and sys.argv[2]:
        if os.path.isfile(sys.argv[2]):
           dbHost = sys.argv[2] # will use it as sqlite
           dbName = ""
           dbPort = ""
        else:
           dbName,dbHost,dbPort,dbSock=es_init.decodeHostNameString(sys.argv[2])
           
    grade  = "daq" ### UPDATE
    if len(sys.argv)==4 and sys.argv[3]:
        grade = sys.argv[3]

    if dbName=="EventStore" and os.environ['USER']!='pass2':   ### UPDATE
       print "To do run truncation in EventStore you need to be 'pass2'"  ### UPDATE
       sys.exit(1) 

    fLines = open(iFile,'r').readlines()
    run    = string.split((fLines[-1]))[1]
    runBase= "%s00"%run[:4]
            
    if dbHost and not dbName:
        try:
           db = sqlite.connect(dbHost)
        except:
           print "Fail to connect to",dbHost
        cu = db.cursor()
    else:
        # authenticate and open MySQLdb connection
        uFileName = os.path.normpath(os.environ["HOME"]+"/.esdb.conf")
        account   = open(uFileName).read()
        userName,userPass = string.split(string.split(account)[0],":")

        try:
           db = MySQLdb.connect(host=dbHost,port=dbPort,user=userName,passwd=userPass,db=dbName)
        except:
           print "Fail to connect to %s@%s:%s"%(dbName,dbHost,dbPort)
           raise

        cu = db.cursor()
        cu.execute("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED")	
        cu.execute("SET AUTOCOMMIT=0")

    # we need to get recent timeStamp and output directory from ESDB
    query = "SELECT MAX(timeStamp) FROM Version WHERE grade='%s'"%grade
    cu.execute(query)
    tup   = cu.fetchone()
    tStamp= ""
    try:
       tStamp=tup[0]
    except:
       print "No time stamp found in ESDB for grade 'grade'"%grade
       sys.exit(1)
    query = """SELECT DISTINCT fileName FROM Location,Version,FileID WHERE 
            Location.graphid=Version.graphid AND Version.grade='%s' AND 
            run='%s' AND FileID.fileId=Location.locationFileId"""%(grade,run)
    cu.execute(query)
    tup   = cu.fetchone()
    oDir  = ""
    try:
       oDir=os.path.split(tup[0])[0]
    except:
       print "No files found in ESDB for run='%s'"%run
       sys.exit(1)
    # find out svName and graphid for given grade and timeStamp
    query = """SELECT DISTINCT svName,Version.graphid FROM Version,SpecificVersion,GraphPath 
            WHERE grade='%s' AND timeStamp='%s' AND Version.graphid=GraphPath.graphid 
            AND GraphPath.svid=SpecificVersion.svid"""%(grade,tStamp)
    cu.execute(query)
    tup   = cu.fetchall()
    if len(tup)>1:
       print "Found multiple dataVersionName's for grade '%s'"%grade
       print tup
       sys.exit(1)
     
    svName= ""
    graphid=""
    try:
       svName=tup[0][0]
       graphid=tup[0][1]
    except:
       print "No dataVersionName found in ESDB for grade '%s'"%grade
       sys.exit(1)

    # check if any truncated run already exists in DB
    query = """SELECT fileName from KeyFile,FileID WHERE run='%s' AND graphid='%s'
            AND KeyFile.keyFileId=FileID.fileId"""%(run,graphid)
    cu.execute(query)
    tup   = cu.fetchall()
    if len(tup)!=1:
       print "Run '%s' already repaired in ESDB"%run
       print "Here the list of key files covered this run"
       for item in tup:
           print item[0]
       sys.exit(0)

    # check that ESTOOLKIT environment is set up
    try:
       path= os.environ['ESTOOLKIT']
    except:
       print "Please define ESTOOLKIT environment to point to your version of toolkit"
       raise "Fail to find ESTOOLKIT"
    # form a script to do injection of truncated run
    DB = ""
    if dbHost and not dbName:
       DB = dbHost
    else:
       DB = "%s@%s:%s"%(dbName,dbHost,dbPort)
    script = """
    #!/bin/bash

    export ESTOOLKIT=%s
    export OUTDIR=%s

    export ESEXE="$ESTOOLKIT/ESBuilder -output $OUTDIR -time %s -grade %s -view truncated -dataVersionName %s -db %s -logFile stdout"

    echo "Executing: $ESEXE"
    $ESEXE -add %s
    """%(path,oDir,tStamp,grade,svName,DB,iFile)

    print script

    # write out and execute script
    fileName = "trunc_run%s_%s.sh"%(run,int(time.mktime(time.localtime())))
    oFile = open(fileName,'w')
    oFile.write(script)
    oFile.close()
    res   = popen2.Popen4("sh %s"%fileName)
    res.wait()
    result= res.fromchild.read()
    print result
    os.remove(fileName)


    # start transaction
    cu.execute('BEGIN')

    # find out key file id of original keyFile
    query = "SELECT DISTINCT keyFileId from KeyFile,Version WHERE run='%s' and view='all' and grade='%s' and Version.graphid=KeyFile.graphid;"%(run,grade)
    #query = "SELECT keyFileId from KeyFile WHERE run='%s' and view='all'"%run
    cu.execute(query)
    tup   = cu.fetchone()
    try:	
       oldId = tup[0]
    except:  
       print "Fail to make a query:",query
       cu.execute('ROLLBACK')
       sys.exit(1)

    # find out key file id of new 'truncated'-view keyFile
    query = "SELECT keyFileId from KeyFile WHERE run='%s' and view='truncated'"%run
    cu.execute(query)
    tup   = cu.fetchone()
    try:
       newId = tup[0]
    except:   
       print "Fail to make a query:",query
       cu.execute('ROLLBACK')
       sys.exit(1)

    # move original keyFile to new view='all-problem'
    query = "UPDATE KeyFile SET view='all-problem' WHERE keyFileId='%s'"%oldId
    cu.execute(query)
            
    # move 'truncated'-view keyFile view to new view='all-problem'
    query = "UPDATE KeyFile SET view='all' WHERE keyFileId='%s'"%newId
    cu.execute(query)

    # print out results
    query = "SELECT * from KeyFile WHERE run='%s'"%run
    cu.execute(query)
    tup   = cu.fetchall()
    for item in tup:
        print item

    # in the case of SQLite commit everything
    #db.commit()
    cu.execute('COMMIT')


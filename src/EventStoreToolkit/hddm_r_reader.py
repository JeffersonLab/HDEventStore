#!/usr/bin/env python
#
# Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
"""REST file reader include two parser:
hddmRunParser just scans the REST files and collects all of the runs in the file
hddmParser collects all the syncValues in the file"""

import array, string, re, sys
import pyhddm_r

def hddmRunParser(fileName,what=""):
    """HDDM run parser. Return run/uid list"""
    hddmFile = pyhddm_r.hddm_istream_proxy(fileName)

    ## for now, take the uid from the file name
    uid = int(1)
    matchObj = re.match( r'dana_rest_\d\d\d\d\d_(\d\d\d\d\d\d\d).hddm', hddmFile.getFilename(), re.M|re.I)
    if matchObj:
        uid = int(matchObj.group(1))

    ######
    #print "in hddm_r_reader.py..."
    #print " filename = " + fileName
    #print " uid = " + str(uid)

    #start to read the rest of the file
    runList = []
    uidList = []
    while hddmFile.read():
        run = hddmFile.getRunNumber()
        #uid = 1  
        #uid = hddmFile.getUid()

        #if not run:
        #    break

#        if not run in runList:
#            runList.append(run)
#        if not uid in uidList:
#           uidList.append(uid)

        if len(runList)==0 or (uid!=uidList[-1] or run!=runList[-1]): 
            runList.append(run)
            uidList.append(uid)

    hddmFile.close()

    runList.sort()
    uidList.sort()
    return [runList,uidList]

def hddmParser(fileName,what=""):
    """HDDM file parser. Return a list of run/uid/sync.values in given file"""
    hddmFile = pyhddm_r.hddm_istream_proxy(fileName)

    #print "in hddmParser()..."
    #print "DEBUG: file name = " + fileName

    ## for now, take the uid from the file name
    uid = int(1)
    matchObj = re.match( r'dana_rest_\d\d\d\d\d_(\d\d\d\d\d\d\d).hddm', hddmFile.getFilename(), re.M|re.I)
    if matchObj:
        uid = int(matchObj.group(1))
    #uid = int(0)

    ######
    #print "in hddm_r_reader.py..."
    #print " filename = " + fileName
    #print " uid = " + str(uid)

    #start to read the rest of the file
    runList = []
    uidList = []
    syncList= []
    while hddmFile.read():
        run = hddmFile.getRunNumber()
        event = hddmFile.getEventNumber()
        #uid = 1  
        #uid = hddmFile.getUid()

        #print "DEBUG: read in " + str(run) + " " + str(event)
        
        #if not run:
        #    break
        if run==0 and event==0:
            continue

        if( (event%100000) == 0):
            print "processing event " + str(event)

        # form syncValue
        syncValue = (run,event,uid)
        syncList.append(syncValue)

        if len(runList)==0 or (uid!=uidList[-1] or run!=runList[-1]): 
            runList.append(run)
            uidList.append(uid)

    hddmFile.close()
        	   
    #print "DEBUG: RUNS = " + str(runList)
    if what=="run":
        return runList.sort()
    elif what=="uid":
        return uidList.sort()
    elif what=="syncValue":
        return syncList.sort()
    
    returnList  = [runList,uidList,syncList]
    return returnList

def fileInfo(fileName):
    """Dump content of pds file to stdout"""
    list=hddmParser(fileName,"")
    runList=list[0]
    uidList=list[1]
    svList =list[2]
    print "File",fileName
    print "Number of runs",len(runList),":",
    for r in runList: print r,

if __name__ == "__main__":
    fileInfo(sys.argv[1])

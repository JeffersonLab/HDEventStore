#!/usr/bin/env python
#
# Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
"""EVIO file reader include two parsers:
eviomRunParser just scans the files and collects all of the runs in the file
evioParser collects all the syncValues in the file
note that we assume that the EVIO files are in single block mode"""

import array, string, re, sys
from os import path
import struct
import evio_utils
import evio_dump

def evioRunParser(fileName,what=""):
    """EVIO run parser. Return run/uid list"""

    runList = []
    uidList = []

    evioReader = evio_dump.EVIOFileReader(fileName)
    ( syncValue, offset ) = evioReader.readRecordInfo()
    while syncValue is not None:
        run = syncValue[0]
        uid = syncValue[1]
        if run not in runList:
            runList.append(run)
        if uid not in uidList:
            uidList.append(uid)
        ( syncValue, offset ) = evioReader.readRecordInfo()

    #if len(runList) == 0:
    #    runList = [ filename_run_number ]
    #if len(uidList) == 0:
    #    uidList = [ uid ]

    evioReader.close()
    return [runList,uidList]


def evioParser(fileName,what=""):
    """EVIO file parser. Return a list of run/uid/sync.values in given file"""

    runList = []
    uidList = []
    syncList= []

    evioReader = evio_dump.EVIOFileReader(fileName)
    ( syncValue, offset ) = evioReader.readRecordInfo()
    while syncValue is not None:
        run = syncValue[0]
        uid = syncValue[1]

        syncList.append( syncValue )
        if run not in runList:
            runList.append(run)
        if uid not in uidList:
            uidList.append(uid)

        ( syncValue, offset ) = evioReader.readRecordInfo()

    evioReader.close()

    ## handle special cases
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
    list=evioParser(fileName,"")
    runList=list[0]
    uidList=list[1]
    svList =list[2]
    print "File",fileName
    print "Number of runs",len(runList),":",
    for r in runList: print r,

if __name__ == "__main__":
    fileInfo(sys.argv[1])

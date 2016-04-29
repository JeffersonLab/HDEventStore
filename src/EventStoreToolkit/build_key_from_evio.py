#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004, 2005
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author: Chris D. Jones, Valentin Kuznetsov, 2004
# 
"""A set of tools to build key file out of EVIO file"""

import array, string, re, sys
import evio_dump

def build_key(iFile, oFile, oFileID):
    """Build a key file from a EVIO file"""
    # open the files
    evioReader= evio_dump.EVIOFileReader(evioFile)
    keyFile = open(oFile,'wb')

    ## for now, assume uid = 1, which is always true for data
    uid = int(1)

    # build key file header
    keyHeader = array.array('I')
    keyHeader.fromlist([2718281*256,oFileID])
    keyHeaderOffsetToNRecords=len(keyHeader)*4
    keyHeader.fromlist([0])
    keyHeader.tofile(keyFile)
        
    #now loop over all the records
    #recordsToSync = array.array('I')
    numRecord = int(0)
    outRecord = array.array('I')
    outRecord.fromlist([0,0,0,0])
    try:
        while True:
            (syncValue, recIdx) = evioReader.readRecordInfo()
            if syncValue is None:
                break

            outRecord[0] = syncValue[0]
            outRecord[1] = syncValue[1]
            outRecord[2] = syncValue[2]
            outRecord[3] = 1      ## stream id, hardcoded to 1 (CODA event) for now
            outRecord[4] = recIdx >> 32
            outRecord[5] = recIdx & 0xffffffff
            outRecord.tofile(keyFile)

            numRecord = numRecord + 1

    except EVIOEOF:
        pass

    #pus out last record synchronization info
    #recordsToStore.tofile(keyFile)
    
    # save number of records in the header
    keyFile.seek(keyHeaderOffsetToNRecords)
    nSyncValues = array.array('I')
    nSyncValues.fromlist([numRecord])
    nSyncValues.tofile(keyFile)
    keyFile.close()
    

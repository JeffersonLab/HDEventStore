#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004, 2005
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author: Chris D. Jones, Valentin Kuznetsov, 2004
# 
"""A set of tools to build key file out of HDDM REST file"""

import array, string, re, sys
import pyhddm_r

def build_key(iFile, oFile, oFileID):
    """Build a key file from a HDDM REST file"""
    # open the files
    hddmFile = pyhddm_r.hddm_istream_proxy(iFile)
    keyFile = open(oFile,'wb')

    ## for now, take the uid from the file name
    uid = int(1)
    matchObj = re.match( r'dana_rest_\d\d\d\d\d_(\d\d\d\d\d\d\d).hddm', hddmFile.getFilename(), re.M|re.I)
    if matchObj:
        uid = int(matchObj.group(1))

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
    outRecord.fromlist([0,0,0,0,0,0])
    try:
        while hddmFile.read():
            endReason = ""
            outRecord[0] = hddmFile.getRunNumber()
            outRecord[1] = hddmFile.getEventNumber()
            outRecord[2] = uid
            outRecord[3] = 1      ## stream id, hardcoded to 1 (CODA event) for now
            outRecord[4] = numRecord >> 32
            outRecord[5] = numRecord & 0xffffffff
            outRecord.tofile(keyFile)

            numRecord = numRecord + 1

    except EOFError:
        print endReason
        #break

    #pus out last record synchronization info
    #recordsToStore.tofile(keyFile)
    
    # save number of records in the header
    keyFile.seek(keyHeaderOffsetToNRecords)
    nSyncValues = array.array('I')
    nSyncValues.fromlist([numRecord])
    nSyncValues.tofile(keyFile)
    keyFile.close()
    

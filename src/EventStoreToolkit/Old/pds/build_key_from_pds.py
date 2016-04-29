#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004, 2005
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author: Chris D. Jones, Valentin Kuznetsov, 2004
# 
"""A set of tools to build key file out of PDS data file"""

import array, string, re, sys
import pds_utils

PDSSIGNATURE=3141592 # magic number for PDS format, as it should be
pdsSignature=0       # signature we read from given PDS file

def build_key(iFile, oFile, oFileID):
    """Build a key file from a pds file"""
    #open the pds file
    pdsFile = open(iFile,'rb')
    keyFile = open(oFile,'wb')

    #read the file's header info to make the key's header
    headerHeader = array.array('I')
    headerHeader.fromfile(pdsFile, 3)
    global pdsSignature
    pdsSignature = headerHeader[0]>>8
    pds_utils.pdsSignature=pdsSignature
    if pdsSignature != PDSSIGNATURE: headerHeader.byteswap()

    keyHeader = array.array('I')
    keyHeader.fromlist([2718281*256,oFileID ])

    # now get the names of the streams
    numberOfRecordNameWords = array.array('I')
    numberOfRecordNameWords.fromfile(pdsFile,1)
    if pdsSignature != PDSSIGNATURE: numberOfRecordNameWords.byteswap()
    nameWords = array.array('I')
    nameWords.fromfile(pdsFile, numberOfRecordNameWords[0])
    if pdsSignature != PDSSIGNATURE: nameWords.byteswap()

    #need number of names
    pdsFile.seek(4*4)
    nameChars = array.array('c')
    nameChars.fromfile(pdsFile,numberOfRecordNameWords[0]*4)
    if pdsSignature != PDSSIGNATURE: nameChars.byteswap()
    #print name_list(nameChars)
    streamNamesFromPDS = pds_utils.name_list(nameChars)
    nRecordNames = len( streamNamesFromPDS )
    sortedStreamNames = list(streamNamesFromPDS)
    sortedStreamNames.sort()

    streamNameString =""
    newStreamIndex2OldIndex = []
    oldStreamIndex2NewIndex = [0]*len(streamNamesFromPDS)
    for name in sortedStreamNames:
       newStreamIndex2OldIndex.append(streamNamesFromPDS.index(name))
       oldStreamIndex2NewIndex[newStreamIndex2OldIndex[-1] ] = len(newStreamIndex2OldIndex)-1
       streamNameString = streamNameString+name +"\0"
    streamNameString = streamNameString[:-1]
    while 0 != len(streamNameString) % 4:
       streamNameString = streamNameString + "\0"
    nameWords = array.array('I')
    nameWords.fromstring(streamNameString)


    #need to compute the length of the rest of the header
    keyHeaderLength = numberOfRecordNameWords[0]+1+1+1+1
    keyHeader.fromlist([keyHeaderLength])
    keyHeader.fromlist([nRecordNames])
    keyHeader=keyHeader +numberOfRecordNameWords +nameWords
    #print keyHeader
    keyHeaderOffsetToNRecords=len(keyHeader)*4
    keyHeader.fromlist([0,keyHeaderLength])
    keyHeader.tofile(keyFile)
    
    #grab the rest of the header
    restOfHeader = array.array('I')
    #the header size ignores the first 3 words in the event
    pdsFile.seek( (headerHeader[2]+2)*4-pdsFile.tell(),1 )
    restOfHeader.fromfile(pdsFile, 1)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    if restOfHeader[0] != headerHeader[2]:
       raise "header inconsistent"
    
    #now loop over all the records
    recordsToSync = array.array('I')
    #print nRecordNames
    recordsToSync.fromlist([0xFFFFFFFFL]*nRecordNames)
    #print recordsToSync
    numRecord = 0
    numSyncValues = 0
    lastSyncValue = array.array('I')
    lastSyncValue.fromlist([0,0,0,0])
    isFirstRecord = 1
    while 1:
       try:
	  endReason = ""
	  # the 1st through 5 words are the same between the key file and pds
	  pdsRecordType = array.array('I')
	  pdsRecordType.fromfile(pdsFile,1)
	  if pdsSignature != PDSSIGNATURE: pdsRecordType.byteswap()
	  #translate the type from the index used in pds to key index
	  pdsRecordType[0] = oldStreamIndex2NewIndex[ pdsRecordType[0] ]
	  keyRecord = array.array('I')
	  keyRecord.fromfile(pdsFile,4)
	  if pdsSignature != PDSSIGNATURE: keyRecord.byteswap()
	  #print keyRecord
	  if lastSyncValue != keyRecord:
	     numSyncValues = numSyncValues + 1
	     lastSyncValue = keyRecord
	     if isFirstRecord:
		isFirstRecord = 0
	     else:
		recordsToSync.tofile(keyFile)
	     keyRecord.tofile(keyFile)
	  recordsToSync[pdsRecordType[0]] = numRecord	
	  numRecord = numRecord + 1
    
	  endReason = "bad Record: record size"
	  recordDataLength = array.array('I')
	  recordDataLength.fromfile(pdsFile,1)
	  if pdsSignature != PDSSIGNATURE: recordDataLength.byteswap()
	  endReason = "bad Record: record (type "+str(keyRecord[0])+") (size " + str(recordDataLength[0]) +")"
    
	  pdsFile.seek(recordDataLength[0]*4,1)
	  
       except EOFError:
#print endReason
	 break
    #pus out last record synchronization info
    recordsToSync.tofile(keyFile)
    
    keyFile.seek(keyHeaderOffsetToNRecords)
    nSyncValues = array.array('I')
    nSyncValues.fromlist([numSyncValues])
    nSyncValues.tofile(keyFile)


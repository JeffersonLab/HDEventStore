#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""PDS file reader include two parser:
pdsRunParser only scan PDS files and collect all stored runs
pdsParser is a full parser which collect syncValues and all proxies"""

import array, string, re, sys
import pds_utils

PDSSIGNATURE=3141592 # magic number for PDS format, as it should be
pdsSignature=0       # signature we read from given PDS file

def printProxiesInAllStreams(streamProxyList):
    """Print proxies in all streams"""
    s = ""
    for x in streamProxyList:
        s = s+"\n===> List of proxies in '%s'\n"%x[0]
	s = s+pds_utils.printProxies(x[1])
    return s

def pdsRunParser(file,what=""):
    """PDS run parser. Return run/uid list"""
    fileDesc = open(file,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)
    pdsSignature=headerHeader[0]>>8
    pds_utils.PDSSIGNATURE=PDSSIGNATURE
    pds_utils.pdsSignature=pdsSignature
    if pdsSignature != PDSSIGNATURE: headerHeader.byteswap()
  
    #grab the rest of the header
    restOfHeader = array.array('I')
    #the header size ignores the first 3 words in the event
    restOfHeader.fromfile(fileDesc, headerHeader[2] -fileDesc.tell()/4 +3)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    if restOfHeader[-1] != headerHeader[2]:
       raise "header inconsistent"

    #start to read the rest of the file
    runList = []
    uidList = []
    while 1:
       try:
	  endReason = ""
	  recordHeader = array.array('I')
	  recordHeader.fromfile(fileDesc,6)
	  if pdsSignature != PDSSIGNATURE: recordHeader.byteswap()
 
	  # read run record
	  runNumber = recordHeader[1]
	  uid = ((recordHeader[4]<<32)|recordHeader[3])
	  if len(runList)==0 or (uid!=uidList[-1] or runNumber!=runList[-1]): 
	     runList.append(runNumber)
	     uidList.append(uid)

	  recordSize = recordHeader[5]
	  # read the record data
	  recordData = array.array('I')
	  recordData.fromfile(fileDesc,recordSize)

	  currentPosition = fileDesc.tell()
	  # read last word and last record
	  w_size = 4               # in bytes, 32 bit.
	  fileDesc.seek(-w_size,2) # read one last word (2 is offset from the end of file)
	  recordWord = array.array('I')
	  recordWord.fromfile(fileDesc,1)
	  if pdsSignature != PDSSIGNATURE: recordWord.byteswap()
	  lastWord_recordSize=recordWord[0]
	  recordSizeInWords=recordWord[0]+1+5
	  fileDesc.seek(-recordSizeInWords*w_size,2)
	  recordHeader = array.array('I')
	  recordHeader.fromfile(fileDesc,6)
	  if pdsSignature != PDSSIGNATURE: recordHeader.byteswap()
	  firstWord_recordSize = recordHeader[5]
	  if firstWord_recordSize!=lastWord_recordSize:
	     print "Last record in file '%s' is corrupted"%fileDesc.name
	     print "Last  32-bit word (record size):",lastWord_recordSize
	     print "First 32-bit word (record size):",firstWord_recordSize
	     return [(),()]
	     
	  # read run record
	  lastRun = recordHeader[1]
	  lastUid = ((recordHeader[4]<<32)|recordHeader[3])
	
	  # jump to the end of file and read run number from there,
	  # if different keep going
#          numberOfBytesToLastRunWord = (5+recordSize)*4
#          fileDesc.seek(-numberOfBytesToLastRunWord,2)
#          recordData = array.array('I')
#          recordData.fromfile(fileDesc,4)
#          if pdsSignature != PDSSIGNATURE: recordData.byteswap()
#          lastRun = recordData[0]
#          lastUid = ((recordData[3]<<32)|recordData[2])
#          print "first",runNumber,uid
#          print "last ",lastRun,lastUid
	  if lastRun==runNumber and lastUid==uid:
	     return [runList,uidList]
	  else:
	    fileDesc.seek(currentPosition)
	  
       except EOFError:
	  break
    runList.sort()
    uidList.sort()
    return [runList,uidList]

def pdsParser(file,what=""):
    """PDS file parser. Return a list of run/uid/sync.values/proxies in given file"""
    fileDesc = open(file,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)
#    global pdsSignature
    pdsSignature=headerHeader[0]>>8
    pds_utils.PDSSIGNATURE=PDSSIGNATURE
    pds_utils.pdsSignature=pdsSignature
    if pdsSignature != PDSSIGNATURE: headerHeader.byteswap()
    # invoke name_list_from_file with additional argument 1 which means
    # return number of words for those names
    streamNames,streamNamesWords   = pds_utils.name_list_from_file(fileDesc,1)
    shProxyNames,shProxyNamesWords = pds_utils.name_list_from_file(fileDesc,1)
    shNames,shNamesWords           = pds_utils.name_list_from_file(fileDesc,1)
  
    #grab the rest of the header
    restOfHeader = array.array('I')
    #the header size ignores the first 3 words in the event
    restOfHeader.fromfile(fileDesc, headerHeader[2] -fileDesc.tell()/4 +3)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    if restOfHeader[-1] != headerHeader[2]:
       raise "header inconsistent"
    proxiesInStreams = pds_utils.find_proxies_in_streams( restOfHeader, streamNames, shProxyNames)

    # define proxy dictionary dict={'stream':listOfProxiesInThatStream}
    pList = []
    for str in streamNames:
        strIndex = streamNames.index(str)
	pList.append((str,proxiesInStreams[strIndex]))

    posOfLastHeaderWords = fileDesc.tell()

    #start to read the rest of the file
    runList = []
    uidList = []
    syncList= []
    while 1:
       try:
	  endReason = ""
	  recordHeader = array.array('I')
	  recordHeader.fromfile(fileDesc,5)
	  if pdsSignature != PDSSIGNATURE: recordHeader.byteswap()
	  # read stream id and convert it to stream name
	  streamId = int(recordHeader[0])
	  stream   = streamNames[streamId]
 
	  # read uid
	  uid = ((recordHeader[4]<<32)|recordHeader[3])

 	  # form syncValue
 	  syncValue = (recordHeader[1],recordHeader[2],uid,stream)
 	  syncList.append(syncValue)
	   
	  # read run record
	  runNumber = recordHeader[1]
	  if len(runList)==0 or (uid!=uidList[-1] or runNumber!=runList[-1]): 
	     runList.append(runNumber)
	     uidList.append(uid)

	  # read the record data
	  recordDataSize = array.array('I')
	  recordDataSize.fromfile(fileDesc,1)
	  if pdsSignature != PDSSIGNATURE: recordDataSize.byteswap()
#       print "recordDataSize",recordDataSize
      
	  recordData = array.array('I')
	  recordData.fromfile(fileDesc,recordDataSize[0]-1)
	  if pdsSignature != PDSSIGNATURE: recordData.byteswap()
      
	  recordDataEnd = array.array('I')
	  recordDataEnd.fromfile(fileDesc,1)
	  if pdsSignature != PDSSIGNATURE: recordDataEnd.byteswap()

	  if recordDataSize[0] != recordDataEnd[0]:
	     print "Error of reading record data"
	     sys.exit(1)
	  
       except EOFError:
	  break
#    runList.sort()
#    uidList.sort()
#    syncList.sort()
    pList.sort()
    if what=="run":
       return runList
    elif what=="uid":
       return uidList
    elif what=="syncValue":
       return syncList
    elif what=="proxies":
       return pList
    
    returnList  = [runList,uidList,pList,syncList]
    returnList += [streamNames,streamNamesWords]
    returnList += [shProxyNames,shProxyNamesWords]
    returnList += [shNames,shNamesWords]
    returnList += [proxiesInStreams,posOfLastHeaderWords]
    return returnList

def fileInfo(fileName):
    """Dump content of pds file to stdout"""
    list=pdsParser(fileName,"")
    runList=list[0]
    uidList=list[1]
    pList  =list[2]
    svList =list[3]
    print "File",fileName
    print "Number of runs",len(runList),":",
    for r in runList: print r,
    print
    print printProxiesInAllStreams(pList)

if __name__ == "__main__":
    fileInfo(sys.argv[1])

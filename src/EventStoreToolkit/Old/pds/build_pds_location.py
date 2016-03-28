#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004, 2005
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""A set of tools to build location file from PDS data file"""

import array, string, re, sys
import pds_dump, pds_utils, gen_util
import time
import sha #used to build the hash used to see if two location files hold the same data

PDSSIGNATURE=3141592 # magic number for PDS format, as it should be
LOCSIGNATURE=2951413 # magic number for location PDS format, as it should be
pdsSignature=0       # signature we read from given file
locSignature=0       # signature we read from given file

def changeFileIdsInLocFile(locFileName,fileIdList):
    """Change fileIDs in location header to provided id list"""
    
    # define signature of location file
    global LOCSIGNATURE
    pds_utils.LOCSIGNATURE=LOCSIGNATURE

    fileDesc = open(locFileName,'r+w+b')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)

    global locSignature
    locSignature=headerHeader[0]>>8
    pds_utils.locSignature=pdsSignature
    if locSignature != LOCSIGNATURE: headerHeader.byteswap()

    nFiles = array.array('I')
    nFiles.fromfile(fileDesc,1)
    pos = fileDesc.tell()
    if locSignature != LOCSIGNATURE: nFiles.byteswap()
	    
    fileIDs = array.array('I')
    fileIDs.fromfile(fileDesc,2*nFiles[0])
    if locSignature != LOCSIGNATURE: fileIDs.byteswap()
	
    # change file ids by new offset
#    fileIDList = []
#    i = 0
#    while i<len(fileIDs):
#        lower = fileIDs[i]
#        i+=1
#        upper = fileIDs[i]
#        i+=1
#        fileId = gen_util.form64BitNumber(lower,upper)
#        newLower, newUpper = gen_util.lowerUpperBitsOfUID(fileId+fileIdOffset)
#        fileIDList.append(newLower)
#        fileIDList.append(newUpper)
    idList = []
    for fileId in fileIdList:
	lower, upper = gen_util.lowerUpperBitsOfUID(fileId)
        idList.append(lower)
        idList.append(upper)
	
    # seek back to fileID position in order to write new fileIDs
    fileDesc.seek(pos,0)

    # write new fileIDs
    fileArr = array.array('I')
    fileArr.fromlist(idList)
    if locSignature != LOCSIGNATURE: fileArr.byteswap()
    fileArr.tofile(fileDesc)
    fileDesc.close()
    return
	
# Build Location file header
#
def buildLocationHeaderFromDict(streamDataKeysDict, fileIDList):
    """Build a PDS location header, from given components"""
    locationHeader = array.array('I')
    locationHeader.fromlist([LOCSIGNATURE*256])
    # file id for master index
    locationHeader.append(0)
    # remaining number of words in of header
    locationHeader.append(0)
    # associated file list
    fList = []
    for fileId in fileIDList:
        lowerMostId,upperMostId = gen_util.lowerUpperBitsOfUID(fileId)
        fList.append(lowerMostId)
        fList.append(upperMostId)
    fileList = array.array('I')
    # for now only set the low word for the file ID
    fileList.fromlist([len(fileIDList)]+fList)
    locationHeader += fileList

    # put streamNames into header
    streamNames=streamDataKeysDict.keys()
    nameWords  = pds_utils.charArrayOfStreams(streamNames) 
    sortedStreamNames=list(streamNames)
    sortedStreamNames.sort()
#    streamNameString =""
#    for name in sortedStreamNames:
#        streamNameString = streamNameString+name +"\0"
#    streamNameString = streamNameString[:-1]
#    while 0 != len(streamNameString) % 4:
#        streamNameString = streamNameString + "\0"
#    nameWords = array.array('I')
#    nameWords.fromstring(streamNameString)

    nWords = array.array('I')
    nWords.fromlist([len(nameWords)])
    locationHeader += nWords+nameWords

    # add dataKeys and hash into header
    dataKeyArray= array.array('I')
    nProxies = array.array('I')
    nProxies.fromlist([0])
    for stream in sortedStreamNames:
        dataKeys = streamDataKeysDict[stream]
	nProxies[0]   = len(dataKeys)
        dataKeyArray += nProxies
        dataKeyString = ""
        for dataKey in dataKeys:
            dataKeyString+= dataKey[0]+"\0"+dataKey[1]+"\0"+dataKey[2]+"\0"
	    while( len(dataKeyString) % 4 ):
               dataKeyString+="\0"
        dataStringArray= array.array('I')
        dataStringArray.fromstring(dataKeyString)
        dataKeyArray  += dataStringArray

#    emptyHashArray = array.array('I')
#    emptyHashArray.fromlist([0]*5)
    hash = sha.new( dataKeyArray.tostring() ).digest()
#    tempArray = array.array('I')
#    tempArray.fromstring(hash)
#    print "hash1",tempArray.tolist()
#    hash = sha.new( (emptyHashArray+dataKeyArray).tostring() ).digest()
#    tempArray = array.array('I')
#    tempArray.fromstring(hash)
#    print "hash2",tempArray.tolist()
    hashWords = array.array('I')
    hashWords.fromstring(hash)
    locationHeader += hashWords+dataKeyArray

    # pad header so Records begin on 8 byte boundary
    if not (len(locationHeader) % 2):
       locationHeader.fromlist([0])
    headerLength = len(locationHeader)+1-3
    locationHeader.fromlist([headerLength])
    locationHeader[2] = headerLength

    return locationHeader

def getStreamDataKeyDictFromPDS(iPDSFileName):
    """Extract from PDS file streamDataKey dictionary"""

    pdsFile = open(iPDSFileName,'rb')
    pdsHeader = array.array('I')
    pdsHeader.fromfile(pdsFile, 3)
    global pdsSignature
    pdsSignature=pdsHeader[0]>>8
    pds_utils.PDSSIGNATURE=PDSSIGNATURE
    pds_utils.pdsSignature=pdsSignature
    if pdsSignature != PDSSIGNATURE:
       pdsHeader.byteswap()
       needToSwap=1
    else:
       needToSwap=0
   
    # now get the names of the streams
    #  the location file and the PDS file use the same format for the
    #  stream names so just have to copy the info
    numberOfRecordNameWords = array.array('I')
    numberOfRecordNameWords.fromfile(pdsFile,1)
    if pdsSignature != PDSSIGNATURE: numberOfRecordNameWords.byteswap()

    #need number of names
    pdsFile.seek(4*4)
    nameChars = array.array('c')
    nameChars.fromfile(pdsFile,numberOfRecordNameWords[0]*4)
    if pdsSignature != PDSSIGNATURE: nameChars.byteswap()
    streamNames = pds_utils.name_list(nameChars)
    sortedStreamNames = list(streamNames)
    sortedStreamNames.sort()

    #build conversion from new stream index to old index
    newStreamIndex2OldIndex = []
    for name in sortedStreamNames:
        newStreamIndex2OldIndex.append(streamNames.index(name))

    #print streamNames
    shProxyNames = pds_utils.name_list_from_file(pdsFile)
    #print shProxyNames
    #print len(shProxyNames)
    shNames = pds_utils.name_list_from_file(pdsFile)
    #print shNames

    #grab the rest of the header
    restOfHeader = array.array('I')
    #the header size ignores the first 3 words in the event
    restOfHeader.fromfile(pdsFile, pdsHeader[2] -pdsFile.tell()/4 +3)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    #print restOfHeader
    if restOfHeader[-1] != pdsHeader[2]:
      raise "header inconsistent"

    #create the list of 'type' 'usage' 'production' tags for each stream
    def proxySort(a,b):
        #print a, b
        temp = cmp(a[0],b[0])
        if 0 == temp:
	  temp=cmp(a[1],b[1])
	  if 0 == temp:
	     temp=cmp(a[2],b[2])
        return temp
    proxiesInStreams = pds_utils.find_proxies_in_streams( restOfHeader, streamNames, shProxyNames)
#sortProxiesInStreams = proxiesInStreams
    for proxies in proxiesInStreams:
        proxies.sort(proxySort)

    streamDataKeyDict = {}
    for oldIndex in newStreamIndex2OldIndex:
	#print oldIndex
	stream  = streamNames[oldIndex] 
	proxies = proxiesInStreams[oldIndex]
	streamDataKeyDict[stream] = proxies

    return streamDataKeyDict

# Build Location file header and pass it to building routines
#
def buildLocationHeader(iPDSFileName, iFileID):
    """Build a PDS location header, from given pds file name and file id"""
#    print "iPDSFileName",iPDSFileName

    pdsFile = open(iPDSFileName,'rb')
    pdsFileID = int(iFileID)

    pdsHeader = array.array('I')
    pdsHeader.fromfile(pdsFile, 3)
    global pdsSignature
    pdsSignature=pdsHeader[0]>>8
    pds_utils.PDSSIGNATURE=PDSSIGNATURE
    pds_utils.pdsSignature=pdsSignature
    if pdsSignature != PDSSIGNATURE:
       pdsHeader.byteswap()
       needToSwap=1
    else:
       needToSwap=0
   
    locationHeader = array.array('I')
    locationHeader.fromlist([2951413*256])
    #file id for master index
    locationHeader.append(0)
    #remaining number of words in of header
    locationHeader.append(0)
    #associated file list
    fileList = array.array('I')
    # for now only set the low word for the file ID
    fileList.fromlist([1,pdsFileID,0])
    locationHeader =locationHeader + fileList

    # now get the names of the streams
    #  the location file and the PDS file use the same format for the
    #  stream names so just have to copy the info
    numberOfRecordNameWords = array.array('I')
    numberOfRecordNameWords.fromfile(pdsFile,1)
    if pdsSignature != PDSSIGNATURE: numberOfRecordNameWords.byteswap()

    #need number of names
    pdsFile.seek(4*4)
    nameChars = array.array('c')
    nameChars.fromfile(pdsFile,numberOfRecordNameWords[0]*4)
    if pdsSignature != PDSSIGNATURE: nameChars.byteswap()
    streamNames = pds_utils.name_list(nameChars)
    sortedStreamNames = list(streamNames)
    sortedStreamNames.sort()

    #build conversion from new stream index to old index
    newStreamIndex2OldIndex = []
    oldStreamIndex2NewIndex = [0]*len(streamNames)
    streamNameString =""
    for name in sortedStreamNames:
        newStreamIndex2OldIndex.append(streamNames.index(name))
        oldStreamIndex2NewIndex[newStreamIndex2OldIndex[-1] ] = len(newStreamIndex2OldIndex)-1
        streamNameString = streamNameString+name +"\0"
    streamNameString = streamNameString[:-1]
    while 0 != len(streamNameString) % 4:
        streamNameString = streamNameString + "\0"
    nameWords = array.array('I')
    nameWords.fromstring(streamNameString)

    locationHeader = locationHeader + numberOfRecordNameWords+nameWords

    #print streamNames
    shProxyNames = pds_utils.name_list_from_file(pdsFile)
    #print shProxyNames
    #print len(shProxyNames)
    shNames = pds_utils.name_list_from_file(pdsFile)
    #print shNames

    #grab the rest of the header
    restOfHeader = array.array('I')
    #the header size ignores the first 3 words in the event
    restOfHeader.fromfile(pdsFile, pdsHeader[2] -pdsFile.tell()/4 +3)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    #print restOfHeader
    if restOfHeader[-1] != pdsHeader[2]:
      raise "header inconsistent"

    #create the list of 'type' 'usage' 'production' tags for each stream
    def proxySort(a,b):
        #print a, b
        temp = cmp(a[0],b[0])
        if 0 == temp:
	  temp=cmp(a[1],b[1])
	  if 0 == temp:
	     temp=cmp(a[2],b[2])
        return temp
    proxiesInStreams = pds_utils.find_proxies_in_streams( restOfHeader, streamNames, shProxyNames)
#sortProxiesInStreams = proxiesInStreams
    for proxies in proxiesInStreams:
        proxies.sort(proxySort)

    #reserve space for our hash
    dataKeyHashIndex = len(locationHeader)
    dataKeyHashArray = array.array('I')
    dataKeyHashArray.fromlist([0]*5)
    locationHeader += dataKeyHashArray

    maxNProxies = 0
    nProxies = array.array('I')
    nProxies.fromlist([0])
    proxiesArray = []
    for oldIndex in newStreamIndex2OldIndex:
	#print oldIndex
	proxies = proxiesInStreams[oldIndex]
	nProxies[0] = len(proxies)
	if nProxies[0] > maxNProxies:
	   maxNProxies = nProxies[0]
	locationHeader +=nProxies
	datakeys = ""
	#now add each string
	proxyList= []
	for proxy in proxies:
	    proxyList.append(proxy)
	    for key in proxy:
	        datakeys +=key+"\0"
	    while( len(datakeys) % 4 ):
	        datakeys +="\0"
	#print len(datakeys)
	#print datakeys
	#print len(datakeys)
	dataKeysArray=array.array('I')
	dataKeysArray.fromstring(datakeys)
#        proxiesArray+=[dataKeysArray.tolist()]
	proxiesArray+=[proxyList]
	#nProxies[0] = len(dataKeysArray)
	#locationHeader += nProxies
	locationHeader +=dataKeysArray
    #calculate the hash
    hash = sha.new( locationHeader[dataKeyHashIndex+1:].tostring() ).digest()
    #print sha.new( locationHeader[dataKeyHashIndex+1:].tostring() ).hexdigest()
    dataKeyHashArray = array.array('I')
    dataKeyHashArray.fromstring(hash)
    locationHeader[dataKeyHashIndex:dataKeyHashIndex+5]=dataKeyHashArray

    locationHeaderBeforePadding = locationHeader.tolist()
    #pad header so Records begin on 8 byte boundary
    if not (len(locationHeader) % 2):
       locationHeader.fromlist([0])
    headerLength = len(locationHeader)+1-3
    locationHeader.fromlist([headerLength])
    locationHeader[2] = headerLength

    #pad the max number of ASUs to be a multiple of 8
    nASUperRecord = maxNProxies
    while ((nASUperRecord + 4) % 8):
      nASUperRecord +=1
      
    #For each stream, create the ASU to file ID list
    whichFileForStream = []
    whichFileForStreamFake = []
    for proxies in proxiesInStreams:
      whichFile = array.array('B')
      fakeArray = array.array('B')

      whichFile.fromlist([0]*len(proxies)+[255]*(nASUperRecord-len(proxies)))
      for x in xrange(0,len(whichFile)):
          fakeArray.append(255)
      #print whichFile
      whichFileForStream.append(whichFile)
      whichFileForStreamFake.append(fakeArray)

    return (pdsFile,locationHeader,locationHeaderBeforePadding,nameWords,dataKeyHashArray,proxiesArray,streamNames,oldStreamIndex2NewIndex,newStreamIndex2OldIndex,whichFileForStream,whichFileForStreamFake,needToSwap)

# Build location file and write it out
#
def build_pds_location(iPDSFileName, iFileID, iLocationFileName, allList=[]):
    """Build location file from PDS data file"""
    # build location header
    pdsFile,locationHeader,locationHeaderBeforePadding,nameWords,dataKeyHashArray,dataKeysArray,streamNames,oldStreamIndex2NewIndex,newStreamIndex2OldIndex,whichFileForStream,whichFileForStreamFake,needToSwap=buildLocationHeader(iPDSFileName, iFileID)

    #write out our header
    locationFile = open(iLocationFileName,'wb')
    locationHeader.tofile(locationFile)

    #start to read the rest of the file
    fileOffset = array.array('L')
    fileOffset.fromlist([0,0])
    lower32of64=0
    upper32of64=1

    recordType = array.array('I')
    recordType.fromlist([0])
    
    # loop over all the records in the file
    count = 0
    prevIdx = 0
    prevSV=()
    while 1:
      try:
	endReason = ""
	offsetInFile = pdsFile.tell()
	fileOffset[lower32of64] = offsetInFile & 0x00000000FFFFFFFFL
	fileOffset[upper32of64] = offsetInFile >> 32
	recordHeader = array.array('I')
	recordHeader.fromfile(pdsFile,5)
	if pdsSignature != PDSSIGNATURE: recordHeader.byteswap()

	# form a syncValue
	uid = long((recordHeader[4]<<32)|recordHeader[3])
	syncValue=(recordHeader[1],recordHeader[2],uid)
	
	#convert to new indexing scheme
	endReason = "bad Record: record size"
	recordDataLength = array.array('I')
	recordDataLength.fromfile(pdsFile,1)
	if pdsSignature != PDSSIGNATURE: recordDataLength.byteswap()
	
	pdsFile.seek(recordDataLength[0]*4,1)

	# insert true value only for those events who are in tagList,
        # othersize insert fake one ('255')
	fileOffset.tofile(locationFile)
	recordType[0] = oldStreamIndex2NewIndex[ recordHeader[0] ]
	recordType.tofile(locationFile)
	if syncValue==prevSV:
	   whichFileForStream[ recordHeader[0] ].tofile(locationFile)
	   continue
	if len(allList):
	   for idx in xrange(count,len(allList)):
	       sv = allList[idx]
	       if sv==prevSV:
	          break
	       if syncValue==sv:
		  count = idx+1
		  break
	       whichFileForStreamFake[ recordHeader[0] ].tofile(locationFile)
	whichFileForStream[ recordHeader[0] ].tofile(locationFile)
	
	prevSV=syncValue

	
      except EOFError:
	break

def buildLocationFileContent(iPDSFileName, iFileID):
    """Instead of writing directly to file return information about location file
    back to the user. It used by old code of L{ESManager} when building combined location file
    out of multiple PDS files"""
    # build location header
    pdsFile,locationHeader,locationHeaderBeforePadding,nameWords,dataKeyHashArray,dataKeysArray,streamNames,oldStreamIndex2NewIndex,newStreamIndex2OldIndex,whichFileForStream,whichFileForStreamFake,needToSwap=buildLocationHeader(iPDSFileName, iFileID)

    recordType = array.array('I')
    recordType.fromlist([0])
    recTypeCopy = array.array('I')
    fakeOffset = array.array('L')
#    fakeOffset.fromlist([0xffffffffL,0xffffffffL])
    fakeOffset.fromlist([0,0])
   
    # list which we going to write out
    svList         = []
    fileOffsetList = []
    recordTypeList = []
    fileIdList     = []
    
    # loop over all the records in the file
    streamNamesInOrder = []
    count = 0
    while 1:
      try:
        #start to read the rest of the file
        fileOffset = array.array('L')
        fileOffset.fromlist([0,0])
        lower32of64=0
	upper32of64=1

	endReason = ""
#        fileOffset[lower32of64] = pdsFile.tell()
	offsetInFile = pdsFile.tell()
	fileOffset[lower32of64] = offsetInFile & 0x00000000FFFFFFFFL
	fileOffset[upper32of64] = offsetInFile >> 32
	recordHeader = array.array('I')
	recordHeader.fromfile(pdsFile,5)
	if pdsSignature != PDSSIGNATURE: recordHeader.byteswap()

	# form a syncValue
	uid = long((recordHeader[4]<<32)|recordHeader[3])
	syncValue=(recordHeader[1],recordHeader[2],uid)
	
	#convert to new indexing scheme
	endReason = "bad Record: record size"
	recordDataLength = array.array('I')
	recordDataLength.fromfile(pdsFile,1)
	if pdsSignature != PDSSIGNATURE: recordDataLength.byteswap()
	
	pdsFile.seek(recordDataLength[0]*4,1)

	recordType[0] = oldStreamIndex2NewIndex[ recordHeader[0] ]
	stream = streamNames[recordType[0]]
	if not streamNamesInOrder: streamNamesInOrder.append(stream)
	if streamNamesInOrder[-1]!=stream: streamNamesInOrder.append(stream)
	
	svList.append(syncValue)
	fileOffsetList.append(fileOffset.tolist())
	recordTypeList.append(recordType.tolist())
	fileIdList.append(whichFileForStream[recordHeader[0]].tolist())
      except EOFError:
	break
    locationHeaderList = locationHeader.tolist()
    return (locationHeaderBeforePadding, svList, fileOffsetList, recordTypeList, fileIdList, streamNames, streamNamesInOrder)

if __name__ == "__main__":
#    build_pds_location(sys.argv[1],sys.argv[2],sys.argv[3])
    locList, streamNames = buildLocationFileContent(sys.argv[1],sys.argv[2])
    for idx in xrange(0,len(locList)):
        if not idx:
	   print "Location File Header"
	   print locList[idx]
	   continue
	print "Record for run/evt/uid",locList[idx][0]
	print "fileOffset",locList[idx][1]
	print "recordType",locList[idx][2]
	print "list of indecies:",locList[idx][3]

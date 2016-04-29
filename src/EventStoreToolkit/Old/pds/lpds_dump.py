#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author: Chris Jones, Valentin Kuznetsov, 2004
#
"""PDS location file dump and parser routines"""

import array, string, re, sys
import pds_utils

pdsSignature = 0
PDSSIGNATURE=2951413
LOCSIGNATURE=2951413

def getFileIds(locFileName):
    """Return a list of fileIds from location file header"""
    
    # define signature of location file
    global LOCSIGNATURE
    pds_utils.LOCSIGNATURE=LOCSIGNATURE

    fileDesc = open(locFileName,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)

    global locSignature
    locSignature=headerHeader[0]>>8
    pds_utils.locSignature=pdsSignature
    if locSignature != LOCSIGNATURE: headerHeader.byteswap()

    nFiles = array.array('I')
    nFiles.fromfile(fileDesc,1)
    if locSignature != LOCSIGNATURE: nFiles.byteswap()
	
    fileIDs = array.array('I')
    fileIDs.fromfile(fileDesc,2*nFiles[0])
    if locSignature != LOCSIGNATURE: fileIDs.byteswap()
    return fileIDs.tolist()
	
def read_tags( iRestOfHeader, iIndex):
    """In given header search for all available usage and production tags"""
    if 0 == iRestOfHeader[iIndex]:
       return (iIndex+1,"", "", "")
    if not pds_utils.could_be_string(iRestOfHeader, iIndex):
       raise pds_utils.ex_found_bad_word, iRestOfHeader[iIndex:].tostring()

    # character array is transparent to endianess problem, convert it back
    header=iRestOfHeader[iIndex:]
    if pdsSignature != PDSSIGNATURE: header.byteswap()
    charArray = array.array('c', header.tostring())

#   charArray = array.array('c', iRestOfHeader[iIndex:].tostring())
    typeTag   = pds_utils.read_tag(charArray,0)
    usageTag  = pds_utils.read_tag( charArray, len(typeTag)+1)
    productionTag = pds_utils.read_tag( charArray, len(typeTag)+1+len(usageTag)+1)
    #print usageTag + " " + productionTag
    lengthOfString = len(typeTag)+len(usageTag)+len(productionTag) + 3
    if 0 != lengthOfString%4:
       lengthOfString = lengthOfString + 4 - lengthOfString%4
    return ( iIndex + (lengthOfString )/4, typeTag,usageTag, productionTag)

def find_datakeys_in_streams( iRestOfHeader, iStreamNames):
    """Search for data keys in all available streams"""
    #for consistency check, make sure all Proxies are used
    returnValue = []
    for item in iStreamNames:
       returnValue.append([])

    nStreams = len(iStreamNames)
    nFoundStreams = 0
    startIndex  = 0
    continueSearch = 1
    ex_parse_error = "parse error"
    while continueSearch:
       try:
	  #startIndex = find_probable_start(iRestOfHeader, startIndex, len(iStreamNames), 2**8)
#	  print "found start"
	  #print iRestOfHeader[int(startIndex):]
	  if startIndex >= len(iRestOfHeader):
	     raise "couldn't find start"
#	  print "# startIndex: ", startIndex
	  index = startIndex
	  for proxyList in returnValue:
	     numberReadProxies = 0
	     if index >= len(iRestOfHeader):
		raise ex_parse_error, "went too far"
	     numberOfProxies = iRestOfHeader[index]
	     nFoundStreams += 1
#             print "numberOfDataKeys " +str(numberOfProxies)
	     index = index + 1
#             print "%s %i" % ("number of Proxies =", numberOfProxies) 
	     while numberReadProxies != numberOfProxies:
		numberReadProxies = numberReadProxies + 1
		#now read strings
		(index,type, usage, production) = read_tags( iRestOfHeader, index)
#		print "i:",index," type:",type," usage:",usage," production:",production
		#print index
		proxyList.append( [type,usage,production] )
		#print proxyList
	  if nFoundStreams == nStreams:
	    continueSearch = 0
	    continue
    
	  if index +1 != len(iRestOfHeader):
	     raise ex_parse_error, "wrong length"
	  continueSearch = 0
       except (ex_parse_error, pds_utils.ex_found_bad_word):
	  #print sys.exc_type, sys.exc_value
	  #print iRestOfHeader[startIndex:]
	  startIndex = startIndex +1
	  #print startIndex
	  returnValue = []
	  for item in iStreamNames:
	     returnValue.append([])
    #print iRestOfHeader[startIndex:]
    return returnValue
    
def locationFileParser(locFileName):
    """Parse header of location file and read data types"""
    # define signature of location file
    global PDSSIGNATURE
    pds_utils.PDSSIGNATURE=PDSSIGNATURE
	
#    print "locFile",locFileName
    locFile   = open(locFileName,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(locFile, 4)

    # to handle endianess, read pdsSignature
    global pdsSignature
    pdsSignature = headerHeader[0]>>8
    pds_utils.pdsSignature=pdsSignature
    needToSwap = 0
    if pdsSignature != PDSSIGNATURE:
       needToSwap=1
       headerHeader.byteswap()

    # read file identifyer list
    nFiles = headerHeader[3]
    fileIDs= array.array('I')
    fileIDs.fromfile(locFile,2*nFiles)
    if pdsSignature != PDSSIGNATURE: fileIDs.byteswap()

    # read stream name list
    streamNames = pds_utils.name_list_from_file(locFile)

    # read hash
    hash = array.array('I')
    hash.fromfile(locFile,5)
    
    # grab the rest of the header
    restOfHeader = array.array('I')
    # the header size ignores the first 3 words in the event
    restOfHeader.fromfile(locFile, headerHeader[2]-locFile.tell()/4 +3)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    if restOfHeader[-1] != headerHeader[2]:
       self.printError([],"Didn't parse correctly location file")
       sys.exit(1)
    dataKeysInStreams = find_datakeys_in_streams(restOfHeader[:-1], streamNames)
    # form dictionary of pairs {(stream,[datakeys])} 
    oDict = {}
    maxDataKeys=0
    for idx in xrange(0,len(streamNames)):
	oDict[streamNames[idx]]=dataKeysInStreams[idx]
	if len(dataKeysInStreams[idx])>maxDataKeys:
	   maxDataKeys=len(dataKeysInStreams[idx])
	
    nCharsForDataKeysInRecord = maxDataKeys
    while ((nCharsForDataKeysInRecord + 4) %8):
       nCharsForDataKeysInRecord += 1
    recordSize = len(fileIDs)+1+nCharsForDataKeysInRecord # in 32-bit words

    # create pds file Ids
    pdsIDList = []
    for idx in xrange(0,len(fileIDs),2):
        pdsID = (fileIDs[idx+1]<<32)|fileIDs[idx]
	pdsIDList.append(int(pdsID))
    posOfFirstRecord=locFile.tell()
    locFile.close()
    return [streamNames,pdsIDList,oDict,hash,dataKeysInStreams,recordSize,posOfFirstRecord,needToSwap]
  
def getProxies(fileName): 
    """Return list of proxies (data) stored in pds location file"""

    # define signature of location file
    global PDSSIGNATURE
    pds_utils.PDSSIGNATURE=PDSSIGNATURE

    fileDesc = open(fileName,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)

    global pdsSignature
    pdsSignature=headerHeader[0]>>8
    pds_utils.pdsSignature=pdsSignature

    if pdsSignature != PDSSIGNATURE: headerHeader.byteswap()

    nFiles = array.array('I')
    nFiles.fromfile(fileDesc,1)
    if pdsSignature != PDSSIGNATURE: nFiles.byteswap()
	    
    fileIDs = array.array('I')
    fileIDs.fromfile(fileDesc,2*nFiles[0])
    if pdsSignature != PDSSIGNATURE: fileIDs.byteswap()

    streamNames = pds_utils.name_list_from_file(fileDesc)

    # grab the hash
    hash = array.array('I')
    hash.fromfile(fileDesc,5)

    # grab the rest of the header
    restOfHeader = array.array('I')
    # the header size ignores the first 3 words in the event
    restOfHeader.fromfile(fileDesc, headerHeader[2] -fileDesc.tell()/4 +3)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    if restOfHeader[-1] != headerHeader[2]:
       raise "header inconsistent"
    dataKeysInStreams = find_datakeys_in_streams(restOfHeader[:-1], streamNames)
    return (streamNames,dataKeysInStreams)

def decodeLocationRecord(locFile,needToSwap,recordSize,nFiles):
    """Decodes a single location file record.
    @type locFile: string
    @param locFile: location file name
    @type needToSwap: integer
    @param needToSwap: flag to inform if we need to swap bytes in in put loc. files
    @type recordSize: integer
    @param recordSize: size of the record
    @type nFiles: integer
    @param nFiles: number of PDS files which this location files describe
    @rtype: tuple
    @return: (fOffsetList,streamIdx,dataKeyList) where fOffsetList is a list of file offsets,
    streamIdx is an index stream and dataKeyList is a list of proxies for this record.
    """
    recordHeader = array.array('I')
    recordHeader.fromfile(locFile,2*nFiles)
    if needToSwap: recordHeader.byteswap()
    fileOffset  = recordHeader.tolist()
    # convert two 32-bit number to one 64-bit
    fOffsetList = []
    for idx in xrange(0,2*nFiles,2):
        fOffsetList.append(fileOffset[idx])
        fOffsetList.append(fileOffset[idx+1])
#        fOffset = (fileOffset[idx+1]<<32)|fileOffset[idx]
#        fOffsetList.append(long(fOffset))

    recordHeader = array.array('I')
    recordHeader.fromfile(locFile,1)
    if needToSwap: recordHeader.byteswap()
    streamIdx = recordHeader[0]
    
    recordHeader = array.array('B')
    recordHeader.fromfile(locFile,(recordSize-2*nFiles-1))
    if needToSwap: recordHeader.byteswap()
    dataKeyList = recordHeader.tolist()
    return (fOffsetList,streamIdx,dataKeyList)
	
def dump(fileName,verbose=1): 
    """Dump content of location file"""
    sNames,pdsIDList,oDict,hash,dataKeysInStreams,recordSize,posOfFirstRecord,needToSwap=locationFileParser(fileName)
    if needToSwap:
       print "File was produced on another endian machine, byte swapping is enabled"
    print "File identifiers:",
    for x in pdsIDList: print "%d"%x,
    print
    print "StreamNames     :",
    for x in sNames: print x,
    print
    print "File hash       :",
    for tByte in hash:
      print tByte,
    print "\n"
    for idx in xrange(0,len(sNames)):
	print "List of data keys in stream:",sNames[idx]
	print pds_utils.printProxies(dataKeysInStreams[idx])
    locFile = open(fileName,'rb')
    locFile.seek(posOfFirstRecord)
    nFiles  = len(pdsIDList)
    count   = 0
    while 1:
       try:
          fOffsetList,streamIdx,dataKeyList=decodeLocationRecord(locFile,needToSwap,recordSize,nFiles)
	  if verbose:
	     print "************"
	     print "# syncValue:",count,
	     if len(sNames):
	        print sNames[streamIdx]
             else:
                print "\n"
	     for index in xrange(0,len(pdsIDList)):
                fileId = pdsIDList[index]
	        print "fileId "+str(fileId)+" at offset "+str(fOffsetList[index*2])+" "+str(fOffsetList[index*2+1])
             print dataKeyList
          count+=1
       except EOFError:
          break

def dump_old(fileName,verbose=1): 
    """Dump content of pds location file to stdout"""

    # define signature of location file
    global PDSSIGNATURE
    pds_utils.PDSSIGNATURE=PDSSIGNATURE

    fileDesc = open(fileName,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)

    global pdsSignature
    pdsSignature=headerHeader[0]>>8
    pds_utils.pdsSignature=pdsSignature

    if pdsSignature != PDSSIGNATURE:
       print "File was produced on another endian machine, byte swapping is enabled"
#       print "File signature",pdsSignature,"and it should be ",PDSSIGNATURE
    if pdsSignature != PDSSIGNATURE: headerHeader.byteswap()

    nFiles = array.array('I')
    nFiles.fromfile(fileDesc,1)
    if pdsSignature != PDSSIGNATURE: nFiles.byteswap()
#    print "Number of file identifiers:",nFiles
	    
    fileIDs = array.array('I')
    fileIDs.fromfile(fileDesc,2*nFiles[0])
    if pdsSignature != PDSSIGNATURE: fileIDs.byteswap()
#    print "fileIDs",fileIDs

    listOfFileIDs=[]
    for i in xrange(0,len(fileIDs),2):
       lowerMostId=fileIDs[i]
       upperMostId=fileIDs[i+1]
       fileID = ((upperMostId<<32)|lowerMostId)
#       print "fileID",lowerMostId,upperMostId,fileID
       listOfFileIDs.append(fileID)
    print "File identifiers:",
    for x in listOfFileIDs: print "%d"%x,
    print

    streamNames = pds_utils.name_list_from_file(fileDesc)
    print "StreamNames     :",
    for x in streamNames: print x,
    print

    # grab the hash
    hash = array.array('I')
    hash.fromfile(fileDesc,5)
    print "File hash       :",
    for tByte in hash:
      print tByte,
#      print hex(tByte), tByte
    print "\n"

    # grab the rest of the header
    restOfHeader = array.array('I')
    # the header size ignores the first 3 words in the event
    restOfHeader.fromfile(fileDesc, headerHeader[2] -fileDesc.tell()/4 +3)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
#    print restOfHeader
    if restOfHeader[-1] != headerHeader[2]:
       raise "header inconsistent"
    dataKeysInStreams = find_datakeys_in_streams(restOfHeader[:-1], streamNames)
    for idx in xrange(0,len(streamNames)):
        print "List of data keys in stream:",streamNames[idx]
        print pds_utils.printProxies(dataKeysInStreams[idx])
#    print dataKeysInStreams

    maxDataKeys = 0
    for dataKeys in dataKeysInStreams:
       if maxDataKeys < len(dataKeys):
	  maxDataKeys = len(dataKeys)
    # determine size of a record
    nWordsForFileIDsInRecord = nFiles[0]*2
    nCharsForDataKeysInRecord = maxDataKeys
    while ((nCharsForDataKeysInRecord + 4) %8):
       nCharsForDataKeysInRecord += 1

    if verbose:
       print "nWordsForFileIDsInRecord : ",nWordsForFileIDsInRecord
       print "             maxDataKeys : ",maxDataKeys
       print "nCharsForDataKeysInRecord: ",nCharsForDataKeysInRecord

    count = 0
    while 1:
       try:
	  endReason = "End Of File"
	  fileOffsets = array.array('I')
	  fileOffsets.fromfile(fileDesc,nWordsForFileIDsInRecord)
	  if pdsSignature != PDSSIGNATURE: fileOffsets.byteswap()

	  recordType = array.array('I')
	  recordType.fromfile(fileDesc,1)
	  if pdsSignature != PDSSIGNATURE: recordType.byteswap()
	  
	  endReason = "bad Record: fileIDS"
	  if verbose:
	     print "********"
	     print "# syncValue:",count
	     if len(streamNames):
	        print streamNames[recordType[0]]
	     for index in xrange(0,nFiles[0]):
	        print "file: "+str(fileIDs[index*2])+" "+str(fileIDs[index*2+1])+"->"+str(fileOffsets[index*2])+" "+str(fileOffsets[index*2+1])
	  dataKeys = array.array('B')
	  dataKeys.fromfile(fileDesc,nCharsForDataKeysInRecord)
	  if pdsSignature != PDSSIGNATURE: dataKeys.byteswap()
	  if verbose: print dataKeys
	  #for index in xrange(0,nCharsForDataKeysInRecord):
	  #   print "   "+str(index)+" "+str(dataKeys[index])
	  count+=1

       except EOFError:
	  if verbose: print endReason
	  break

if __name__ == "__main__":
    dump(sys.argv[1])
#    dump_old(sys.argv[1])


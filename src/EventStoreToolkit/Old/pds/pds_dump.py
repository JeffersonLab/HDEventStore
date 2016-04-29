#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""PDS file dump tools"""

import array, string, re, sys
import pds_utils

def getPDSHeader(fileName):
    """Return pds header in form of array.array"""
    svName,parents,verList,verDict = decodeVersionInfo(fileName)
	
    PDSSIGNATURE=3141592 # magic number for PDS format, as it should be
    pdsSignature=0       # signature we read from given PDS file

    fileDesc = open(fileName,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)

    # to handle endianess, read pdsSignature
    pdsSignature = headerHeader[0]>>8
    if pdsSignature != PDSSIGNATURE:
       headerHeader.byteswap()

    # grab the rest of the header
    restOfHeader = array.array('I')
    # the header size ignores the first 3 words in the event
    restOfHeader.fromfile(fileDesc, headerHeader[2])
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    if restOfHeader[-1] != headerHeader[2]:
       raise "header inconsistent"
    return headerHeader+restOfHeader
    
def pdsHeaderParser(fileName):
    """Parser header of PDS file"""
    svName,parents,verList,verDict = decodeVersionInfo(fileName)
	
    PDSSIGNATURE=3141592 # magic number for PDS format, as it should be
    pdsSignature=0       # signature we read from given PDS file

    fileDesc = open(fileName,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)

    # to handle endianess, read pdsSignature
    pdsSignature = headerHeader[0]>>8
    pds_utils.pdsSignature=pdsSignature
    pds_utils.PDSSIGNATURE=PDSSIGNATURE

    needToSwap   = 0
    if pdsSignature != PDSSIGNATURE:
       needToSwap= 1
       headerHeader.byteswap()
    streamNames  = pds_utils.name_list_from_file(fileDesc)
    
    shProxyNames = pds_utils.name_list_from_file(fileDesc)
    shNames = pds_utils.name_list_from_file(fileDesc)

    # grab the rest of the header
    restOfHeader = array.array('I')
    # the header size ignores the first 3 words in the event
    restOfHeader.fromfile(fileDesc, headerHeader[2] -fileDesc.tell()/4 +3)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    if restOfHeader[-1] != headerHeader[2]:
       raise "header inconsistent"
    proxiesInStreams = pds_utils.find_proxies_in_streams( restOfHeader, streamNames, shProxyNames)
    oDict = {}
    for idx in xrange(0,len(streamNames)):
	oDict[streamNames[idx]]=proxiesInStreams[idx]
    return (fileDesc,needToSwap,streamNames,oDict)

def dump(fileName,verbose=0,iStream='event'):
    """Dump content of pds file to stdout"""
    svName,parents,verList,verDict = decodeVersionInfo(fileName)
    if not parents:
       parents = 'N/A'
    if svName:
       print "Versioning information:"
       print "-----------------------"
       print "data version name: '%s'"%svName
       print "list of parents  :",parents
	
    PDSSIGNATURE=3141592 # magic number for PDS format, as it should be
    pdsSignature=0       # signature we read from given PDS file

    fileDesc = open(fileName,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)

    # to handle endianess, read pdsSignature
    pdsSignature = headerHeader[0]>>8
    pds_utils.pdsSignature=pdsSignature
    pds_utils.PDSSIGNATURE=PDSSIGNATURE

    if pdsSignature != PDSSIGNATURE:
       print "File was produced on another endian machine, byte swapping is enabled"
    if pdsSignature != PDSSIGNATURE: headerHeader.byteswap()
    print
    print "List of streams in a file:",
    streamNames = pds_utils.name_list_from_file(fileDesc)
    for x in streamNames: print x,
    print
    
    shProxyNames = pds_utils.name_list_from_file(fileDesc)
    shNames = pds_utils.name_list_from_file(fileDesc)
    if verbose:
       print "List of types that storage helper proxy factories have:"
       for x in shProxyNames: print x,
       print "\n\n"
       print "List of types that only have storage helpers"
       for x in shNames: print x,
       print

    # grab the rest of the header
    restOfHeader = array.array('I')
    # the header size ignores the first 3 words in the event
    restOfHeader.fromfile(fileDesc, headerHeader[2] -fileDesc.tell()/4 +3)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    if restOfHeader[-1] != headerHeader[2]:
       raise "header inconsistent"
    proxiesInStreams = pds_utils.find_proxies_in_streams( restOfHeader, streamNames, shProxyNames)
    print
    for idx in xrange(0,len(streamNames)):
        print "List of data keys in stream:",streamNames[idx]
        print pds_utils.printProxies(proxiesInStreams[idx])

    # want to only look at stuff in given stream (default is event stream)
    if not streamNames.count(iStream):
       print "There is no stream %s in %s"%(iStream,fileName)
       return
    eventIndex = streamNames.index(iStream)

    # create a structure to hold our info
    eventProxies = proxiesInStreams[eventIndex]
    accumulatedData = []
    for proxy in eventProxies:
       accumulatedData.append([0,488888888,0])

    # start to read the rest of the file
    numEvents = 0

    firstSV=""
    lastSV =""
    while 1:
       try:
	  endReason = ""
	  pos = fileDesc.tell()
	  recordHeader = array.array('I')
	  recordHeader.fromfile(fileDesc,5)
	  if pdsSignature != PDSSIGNATURE: recordHeader.byteswap()
	
	  uid = ((recordHeader[4]<<32)|recordHeader[3])
	  if not len(firstSV):
	     firstSV="%d/%d/%d"%(recordHeader[1],recordHeader[2],uid)
	  lastSV="%d/%d/%d"%(recordHeader[1],recordHeader[2],uid)

	  if verbose:
	     print "********************"
	     print "position           : %s"%pos
	     print "stream             : "+streamNames[recordHeader[0]]
	     print "SyncValue          : %s/%s/%s"%(recordHeader[1],recordHeader[2],uid)

	  endReason = "bad Record: record size"
	  recordDataLength = array.array('I')
	  recordDataLength.fromfile(fileDesc,1)
	  if pdsSignature != PDSSIGNATURE: recordDataLength.byteswap()
	  if verbose:
	     print "size of record data: "+str(recordDataLength[0])
	  
	  endReason = "bad Record: record (type "+streamNames[recordHeader[0]]+") (size " + str(recordDataLength[0]) +")"
	  begRecPos = fileDesc.tell()
	  recordData = array.array('I')
	  recordData.fromfile(fileDesc, recordDataLength[0])
	  if pdsSignature != PDSSIGNATURE: recordData.byteswap()
	  
	  numEvents = numEvents+1
	  index = 0
	  endIndex = len(recordData)
	  dataInfo     = []
	  dataInfoSize = []
	  lengthOffset = 0
	  while index < endIndex - 1:
	    proxyIndex = int(recordData[index])
	    index = index + 1
	    dataSize =  recordData[index]
	    index = index + int(dataSize) + 1
	    fullDataSize = dataSize+2
	    strData = str(proxiesInStreams[recordHeader[0]][proxyIndex])
	    if lengthOffset<len(strData):
	       lengthOffset=len(strData)
	    dataInfo.append(strData)
	    dataInfoSize.append(fullDataSize)
#            if verbose:
#               print str(proxiesInStreams[recordHeader[0]][proxyIndex]) + " size "+str(fullDataSize)
	    if recordHeader[0] == eventIndex:
		accumulatedData[proxyIndex][0] = accumulatedData[proxyIndex][0]+fullDataSize
		if accumulatedData[proxyIndex][1] > fullDataSize:
		   accumulatedData[proxyIndex][1] = fullDataSize
		if accumulatedData[proxyIndex][2] < fullDataSize:
		   accumulatedData[proxyIndex][2] = fullDataSize
	  if verbose:
	     print
	     for idx in xrange(0,len(dataInfo)):
	         s = dataInfo[idx]
		 print "%s size %s"%(s.ljust(lengthOffset),dataInfoSize[idx])

       except EOFError:
	 if verbose and endReason:
	    print "########################################################"
	    print endReason
	    print "########################################################"
	 break


    index = 0
    if numEvents == 0:
      raise "no events found"

    if verbose:
       print "######### Summary ##########"
       print "Proxies in %s:"%iStream
       for item in eventProxies:
	  print "%s : %i %f %i" % (item, accumulatedData[index][1], float(accumulatedData[index][0])/float(numEvents), accumulatedData[index][2])
	  index = index + 1

    print "First sync value     :",firstSV
    print "Last sync value      :",lastSV
    print "Number of syncValues :",numEvents
    print

def formVersionInfoWord(list):
    """Form version info word from provided list of attributes"""
    versionInfo=[]
    word = ""
    for item in list:
	if item!='\x00':
	   word+=item
	else:
	   if word:
	      if len(versionInfo)==3:
		 word=ord(word)
	      versionInfo.append(word)
	   word = ""
    return versionInfo

def proxyReader(fileDesc,needToSwap,proxyLength):
    """Proxy reader from provided file descriptor"""
    # now let's read proxy's data
    nChars = 0
    word   = ""
    versionInfo = []
    charCounter = 0
    # read first three strings which are splitted by '\x00'
    while len(versionInfo)<3:
        if (proxyLength*4)==charCounter:
	   break
	cData = array.array('c')
	cData.fromfile(fileDesc,1)
	oneChar = cData.tolist()[0]
	charCounter+=1
	nChars+=1
	if oneChar=='\x00':
	   if word: versionInfo.append(word)
	   # add padding 4-len(word)%4 and substruct read oneChar
	   zeros = 4-len(word)%4-1
	   charCounter+=zeros
	   if zeros:
	      cData.fromfile(fileDesc,zeros)
	   word = ""
	   continue
	word+=oneChar
    if (proxyLength*4)==charCounter:
       # we read something else, let's quit
       return ([],{})
    # read as characters one unsigned int word
    ordinalWord = array.array('I')
    ordinalWord.fromfile(fileDesc,1)
    charCounter+=4
    if needToSwap: ordinalWord.byteswap()
    versionInfo+=ordinalWord.tolist()
    # read 1 word which consist of first 5-bits as padCounter and rest as object Counter
    objData = array.array('I')
    objData.fromfile(fileDesc,1)
    charCounter+=4
    if needToSwap: objData.byteswap()
    obj = objData[0]
    padCount = int(obj & 0x1F)
    objCount = int(obj >> 5)
#    print "padCount",padCount,"objCount",objCount
    if objCount:
       word  = ""
       words = []
       while len(words)<objCount:
	   cData = array.array('c')
	   cData.fromfile(fileDesc,1)
	   oneChar = cData.tolist()[0]
	   charCounter+=1
	   if oneChar=='\x00':
	      if word: words.append(word)
	      # add padding 4-len(word)%4 and substruct read oneChar
	      zeros = 4-len(word)%4-1
	      charCounter+=zeros
	      if zeros:
	         cData.fromfile(fileDesc,zeros)
	      word = ""
	      continue
	   word+=oneChar
       versionInfo+=words
    if (proxyLength*4)!=charCounter:
       raise "Bad read of proxy data, proxyLength=%s and we read %s bytes"%(proxyLength*4,charCounter)
    # form versionInfo dictionary {ordinal:svName}
    versionInfoDict = {versionInfo[3]:versionInfo[1]}
    return (versionInfo,versionInfoDict)
    
def decodeVersionInfo(fileName):
    """Decode VersionInfo from beginrun record. 
    VersionInfo consists of:
       - softwareRelease : string
       - specificVersionName : string
       - configurationHash : string
       - ordinal : unsigned int no packing
       - ancestors : container of string
    So, it always grows, e.g. post-p2 file will contains two VersionInfo's, one
    for itself and one for it's parent. So, the underlying algorithm creates
    a list of VersionInfo's in the following format:
    [(childTag,[softRel,svName,hash,id,parent1,parent2,...]),(parentTag,[...])]
    This method returns (svName,[listOfParents]) tuple
    """
    emptyTuple  = ('','','','')
    PDSSIGNATURE=3141592 # magic number for PDS format, as it should be
    pdsSignature=0       # signature we read from given PDS file

    fileDesc = open(fileName,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)

    # to handle endianess, read pdsSignature
    pdsSignature = headerHeader[0]>>8
    pds_utils.pdsSignature=pdsSignature
    pds_utils.PDSSIGNATURE=PDSSIGNATURE

    needToSwap = 0
    if pdsSignature != PDSSIGNATURE:
       needToSwap = 1
       headerHeader.byteswap()
    streamNames = pds_utils.name_list_from_file(fileDesc)
    shProxyNames = pds_utils.name_list_from_file(fileDesc)
    shNames = pds_utils.name_list_from_file(fileDesc)

    # grab the rest of the header
    restOfHeader = array.array('I')
    # the header size ignores the first 3 words in the event
    restOfHeader.fromfile(fileDesc, headerHeader[2] -fileDesc.tell()/4 +3)
    if needToSwap: restOfHeader.byteswap()
    if restOfHeader[-1] != headerHeader[2]:
       raise "header inconsistent"
    proxiesInStreams = pds_utils.find_proxies_in_streams( restOfHeader, streamNames, shProxyNames)

    # want to only look at stuff in beginrun stream
    if not streamNames.count('beginrun'):
       return emptyTuple
    eventIndex = streamNames.index('beginrun')

    # create a structure to hold our info
    eventProxies    = proxiesInStreams[eventIndex]
    accumulatedData = []
    foundVersionInfo= 0
    for proxy in eventProxies:
	accumulatedData.append([0,488888888,0])
	if proxy[0]=='VersionInfo':
	   foundVersionInfo=1
           break
    # check if VersionInfo is present in a proxy list
    if not foundVersionInfo:
       return emptyTuple

    # start to read the rest of the file
    numEvents = 0
    nWordSize = 4 # we use to use 32-bit words, which is 4 bytes
    firstSV   =""
    lastSV    =""
    versionInfoList = []
    versionInfoDict = {}
    while 1:
       try:
	  endReason = ""
	  recordHeader = array.array('I')
	  recordHeader.fromfile(fileDesc,5)
	  if needToSwap: recordHeader.byteswap()
	
	  uid = ((recordHeader[4]<<32)|recordHeader[3])
	  if not len(firstSV):
	     firstSV="%d/%d/%d"%(recordHeader[1],recordHeader[2],uid)
	  lastSV="%d/%d/%d"%(recordHeader[1],recordHeader[2],uid)

	  endReason = "bad Record: record size"
	  recordDataLength = array.array('I')
	  recordDataLength.fromfile(fileDesc,1)
	  if needToSwap: recordDataLength.byteswap()
	  
	  if recordHeader[0]>len(streamNames)-1:
	     break
	  endReason = "bad Record: record (type "+streamNames[recordHeader[0]]+") (size " + str(recordDataLength[0]) +")"
	  curStream = streamNames[recordHeader[0]]
	  if curStream!='beginrun':
	     continue
	  begRecPos = fileDesc.tell()
	  recordData= array.array('I')
	  recordData.fromfile(fileDesc, recordDataLength[0]-1)
	  if needToSwap: recordData.byteswap()
	  testLastWord= array.array('I')
	  testLastWord.fromfile(fileDesc,1)
	  if needToSwap: testLastWord.byteswap()
	  endRecPos = fileDesc.tell()
	  
	  ############ remove later this block, it's a full dataRecord
	  fileDesc.seek(begRecPos)
	  cData = array.array('c')
	  cData.fromfile(fileDesc, (recordDataLength[0])*nWordSize)
	  ################
	
	  fileDesc.seek(begRecPos)
	  nReadWords = 0
	  while nReadWords<(recordDataLength[0]-1):
	     # let's read two words: proxy index and number of words used for Proxy's data
	     proxyData = array.array('I')
	     proxyData.fromfile(fileDesc,2)
	     nReadWords+=2
	     if needToSwap: proxyData.byteswap()
	     proxyIndex = proxyData[0]
	     productionTag = eventProxies[proxyIndex][2]
	     proxyLength= proxyData[1]
	     if eventProxies[proxyIndex][0]=='VersionInfo':
		# now let's read proxy's data
		versionInfo, dict = proxyReader(fileDesc,needToSwap,proxyLength)
		for key in dict.keys():
		    if versionInfoDict.has_key(key):
		       if not versionInfoDict[key].count(dict[key]):
			  versionInfoDict[key]+= [dict[key]]
		    else:
		       versionInfoDict[key] = [dict[key]]
		if not versionInfoList.count((productionTag,versionInfo)):
		   versionInfoList.append((productionTag,versionInfo))
             else:
	        fileDesc.seek(proxyLength*4,1)
	     nReadWords+=proxyLength
	  # read last word of the recordData
	  lastWord = array.array('I')
	  lastWord.fromfile(fileDesc,1)
	  if needToSwap: lastWord.byteswap()
	  if lastWord[0]!=recordDataLength[0] or fileDesc.tell()!=endRecPos:
	     raise "While decoding proxies we went too far"
          # once we reach this point, we found VersionInfo in beginrun record
          break
       except EOFError:
	 break
    keys   = versionInfoDict.keys()
    maxKey = max(keys)
    svName = versionInfoDict[maxKey][0]
    keys.remove(maxKey)
    parents= ""
    if len(keys):
       parents= versionInfoDict[max(keys)]
    return (svName,parents,versionInfoList,versionInfoDict)

# Read one record in PDS file and return
# syncValue, fileOffset, recordType, fileId
#
def readPDSRecord(pdsFile,needToSwap):
    """Read one record from PDS file, input parameter 'pdsFile' is open PDS file at
    position of first record"""
    recordType = array.array('I')
    recordType.fromlist([0])
   
    # list which we going to write out
    syncValue      = ()
    fileOffsetList = []
    recordTypeList = []
    fileIdList     = []
    
    # loop over all the records in the file
    streamNamesInOrder = []
    try:
	#start to read the rest of the file
	fileOffset = array.array('L')
	fileOffset.fromlist([0,0])
	lower32of64=0
	upper32of64=1

	offsetInFile = pdsFile.tell()
	fileOffset[lower32of64] = offsetInFile & 0x00000000FFFFFFFFL
	fileOffset[upper32of64] = offsetInFile >> 32
	fileOffsetList = fileOffset.tolist()
	recordHeader = array.array('I')
	recordHeader.fromfile(pdsFile,5)
	if needToSwap: recordHeader.byteswap()

	recordIndex = recordHeader[0]
	# form a syncValue
	uid = long((recordHeader[4]<<32)|recordHeader[3])
	syncValue=(recordHeader[1],recordHeader[2],uid)
	
	#convert to new indexing scheme
	recordDataLength = array.array('I')
	recordDataLength.fromfile(pdsFile,1)
	if needToSwap: recordDataLength.byteswap()
	
#        pdsFile.seek(recordDataLength[0]*4,1)
	recordData = array.array('I')
	recordData.fromfile(pdsFile,recordDataLength[0]-1)
	if needToSwap: recordData.byteswap()
    
	recordDataEnd = array.array('I')
	recordDataEnd.fromfile(pdsFile,1)
	if needToSwap: recordDataEnd.byteswap()

	if recordDataLength[0] != recordDataEnd[0]:
	   print "Error of reading record data from '%s'"%pdsFile.name
	   sys.exit(1)

    except EOFError:
	return ""
    return (fileOffsetList,recordHeader,recordDataLength+recordData+recordDataEnd)
    
def getPDSRecordInfo(pdsFile,needToSwap):
    """Read information from one PDS record
    @type pdsFile: file descriptor
    @param pdsFile: pds file descriptor
    @type needToSwap: integer
    @param needToSwap: flag which indicates if we need to swap bytes in pdsFile
    @rtype: tuple
    @return: (syncValue, fileOffsetList, recordIndex)
    """
    pdsRecord = readPDSRecord(pdsFile,needToSwap)
    if not pdsRecord:
	return ((),[],-pds_utils.MAGIC_NUMBER)
    fileOffsetList = pdsRecord[0]
    recordHeader   = pdsRecord[1]
    recordData     = pdsRecord[2]
    
    recordIndex    = recordHeader[0]
    # form a syncValue
    uid = long((recordHeader[4]<<32)|recordHeader[3])
    syncValue=(recordHeader[1],recordHeader[2],uid)
    return (syncValue, fileOffsetList, recordIndex)
    
def readPDSRecord_old(pdsFile,needToSwap):
    """Read one record from PDS file, input parameter 'pdsFile' is open PDS file at
    position of first record"""
    recordType = array.array('I')
    recordType.fromlist([0])
   
    # list which we going to write out
    syncValue      = ()
    fileOffsetList = []
    recordTypeList = []
    fileIdList     = []
    
    # loop over all the records in the file
    streamNamesInOrder = []
    try:
	#start to read the rest of the file
	fileOffset = array.array('L')
	fileOffset.fromlist([0,0])
	lower32of64=0
	upper32of64=1

	offsetInFile = pdsFile.tell()
	fileOffset[lower32of64] = offsetInFile & 0x00000000FFFFFFFFL
	fileOffset[upper32of64] = offsetInFile >> 32
	fileOffsetList = fileOffset.tolist()
	recordHeader = array.array('I')
	recordHeader.fromfile(pdsFile,5)
	if needToSwap: recordHeader.byteswap()

	recordIndex = recordHeader[0]
	# form a syncValue
	uid = long((recordHeader[4]<<32)|recordHeader[3])
	syncValue=(recordHeader[1],recordHeader[2],uid)
	
	#convert to new indexing scheme
	recordDataLength = array.array('I')
	recordDataLength.fromfile(pdsFile,1)
	if needToSwap: recordDataLength.byteswap()
	
#        pdsFile.seek(recordDataLength[0]*4,1)
	recordData = array.array('I')
	recordData.fromfile(pdsFile,recordDataLength[0]-1)
	if needToSwap: recordData.byteswap()
    
	recordDataEnd = array.array('I')
	recordDataEnd.fromfile(pdsFile,1)
	if needToSwap: recordDataEnd.byteswap()

	if recordDataLength[0] != recordDataEnd[0]:
	   print "Error of reading record data from '%s'"%pdsFile.name
	   sys.exit(1)

    except EOFError:
#        print "EOF",pdsFile.name
	return ((),[],-pds_utils.MAGIC_NUMBER)
    return (syncValue, fileOffsetList, recordIndex)
  
class PDSFileReader:
    """Base class to read PDS file"""
    def __init__(self,fileName):
	pdsFile,needToSwap,sNames,dataInStreams = pdsHeaderParser(fileName)
	self.pdsFile= pdsFile
	self.swap   = needToSwap
	self.sNames = sNames
	self.streamDataDict = dataInStreams
        self.posFirstRecord = self.pdsFile.tell()
    def setPosition(self,pos):
        """Set position of pds file descriptor.
        @type pos: integer
        @param pos: position of the file
        @rtype: none
        @return: seek to provided position in a file"""
	self.pdsFile.seek(pos)
    def fileDesc(self):
        """PDS file descriptor.
        @rtype: file descriptor
        return: pds file descriptor"""
	return self.pdsFile
    def backToFirstRecord(self):
        """PDS file descriptor of the first record in a file.
        @rtype: none
        @return: seek to the first record in a file"""
        self.pdsFile.seek(self.posFirstRecord)
    def readLastRecordInfo(self):
        """Return last record in pds file. Record info is retrieved by L{getPDSRecordInfo} method"""
	pos = self.pdsFile.tell()
        w_size = 4 # in bytes, 32 bit.
	self.pdsFile.seek(-w_size,2) # 2 is offset from the end of file
	recordWord = array.array('I')
	recordWord.fromfile(self.pdsFile,1)
	if self.swap:
	   recordWord.byteswap()
	recordSizeInWords=recordWord[0]+1+5
	self.pdsFile.seek(-recordSizeInWords*w_size,2)
	content = getPDSRecordInfo(self.pdsFile,self.swap)
        syncValue  = content[0]
        fileOffset = content[1]
        recIdx     = content[2]
        stream     = self.sNames[recIdx]
	self.pdsFile.seek(pos)
	return (syncValue,fileOffset,recIdx,stream)
    def readRecordInfo(self):
        """Return current record information, we use L{getPDSRecordInfo} method"""
	syncValue,fileOffset,recIdx = getPDSRecordInfo(self.pdsFile,self.swap)
#        print "readRecordInfo",syncValue,fileOffset,recIdx
        stream = ""
        if syncValue:
           stream = self.sNames[recIdx]
        return (syncValue,fileOffset,recIdx,stream)
    def readRecord(self):
        """Read one record in pds file, we use L{readPDSRecord} method"""
        return readPDSRecord(self.pdsFile,self.swap)
    def streamNames(self):
        """Return stream names"""
	return self.sNames
    def needToSwap(self):
        """Return a flag if we pds file was produced on another endian node"""
	return self.swap
    def dataInStreams(self):
        """Return a dictionary of stream:data"""
	return self.streamDataDict
    def newOldIndecies(self):
	"""Build conversion from new stream index to old index"""
	sortedStreamNames = list(self.sNames)
	sortedStreamNames.sort()
	newStreamIndex2OldIndex = []
	oldStreamIndex2NewIndex = [0]*len(self.sNames)
	for name in sortedStreamNames:
	    newStreamIndex2OldIndex.append(self.sNames.index(name))
	    oldStreamIndex2NewIndex[newStreamIndex2OldIndex[-1] ] = len(newStreamIndex2OldIndex)-1
	return (oldStreamIndex2NewIndex,newStreamIndex2OldIndex)


if __name__ == "__main__":
#    dump(sys.argv[1],1,'beginrun')
    svName,parents,verList,verDict = decodeVersionInfo(sys.argv[1])
    print "svName ",svName
    print "parents",parents
    print verList
    print verDict
#    print getPDSHeader(sys.argv[1])

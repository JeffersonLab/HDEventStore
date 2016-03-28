#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author: Chris Jones, Valentin Kuznetsov, 2004
#
"""PDS file utilities (read number of proxies, etc.)"""

import array, string, re, sys

MAGIC_NUMBER=2**32
PDSSIGNATURE=3141592 # magic number for PDS format, as it should be
pdsSignature=0       # signature we read from given PDS file

def charArrayOfStreams(streamNames):
    """Return an array Int32 words corresponding to provide stream names"""
    sortedStreamNames=list(streamNames)
    sortedStreamNames.sort()
    streamNameString =""
    for name in sortedStreamNames:
        streamNameString = streamNameString+name +"\0"
    streamNameString = streamNameString[:-1]
    while 0 != len(streamNameString) % 4:
        streamNameString = streamNameString + "\0"
    nameWords = array.array('I')
    nameWords.fromstring(streamNameString)
    return nameWords

def name_list( iArray ):
    """Construct a name list from given array"""
    startIndex = 0
    endIndex = 0
    index = 0
    returnValue = []
    for char in iArray.tolist():
       if char == '\000':
	  endIndex = index
	  if startIndex != endIndex:
	     returnValue.append( iArray[startIndex:endIndex].tostring() )
	  startIndex = index +1
       index = index + 1
    return returnValue

def name_list_from_file( fileDesc, withNumberOfWords=""):
    """Form a name list"""
    numberOfWords = array.array('I')
    numberOfWords.fromfile(fileDesc,1)
#    print "signatures",pdsSignature,PDSSIGNATURE
    if pdsSignature != PDSSIGNATURE: numberOfWords.byteswap()
#    print "#Words ", numberOfWords, len(numberOfWords)
    nameWords = array.array('c')
    nameWords.fromfile(fileDesc, 4*numberOfWords[0])
    if pdsSignature != PDSSIGNATURE: nameWords.byteswap()
    if withNumberOfWords: return [name_list(nameWords),numberOfWords]
    return name_list( nameWords)

def could_be_string(iRestOfHeader, iStartIndex):
    """Determine if position in a header could be a string"""
    if 0 == iRestOfHeader[iStartIndex]:
       return 1
    #now create a character array
    header=iRestOfHeader[iStartIndex:]
    if pdsSignature != PDSSIGNATURE: header.byteswap()
    charArray = array.array('c', header.tostring())

#    charArray = array.array('c', iRestOfHeader[iStartIndex:].tostring())
    #find first occurance of 0
    #print charArray
    startIndex = 0
    firstZeroIndex= len(charArray)
    index = 0
    for character in charArray:
       if character == '\000' :
	  firstZeroIndex = index
	  break
       index = index +1

    if 0 == firstZeroIndex:
       #could be blank usage tag and actual production tag
       index = 0
       startIndex = 1
       #print charArray[startIndex:]
       for character in charArray:
	  if index == 0:
	     index = index + 1
	     continue
	  if character == '\000' :
	     firstZeroIndex = index
	     break
	  index = index +1
       if 0 == firstZeroIndex:
	  return 0
       
    #are we dealing with legal characters?
    #print charArray[startIndex:firstZeroIndex].tostring()
    #print 'doing match'
    matchCheck = re.match('[a-zA-Z]+[a-zA-Z<>0-9+-]*',charArray[startIndex:firstZeroIndex].tostring())
    if matchCheck == None:
       return 0
    #print firstZeroIndex
    if matchCheck.end() != firstZeroIndex - startIndex:
       return 0
    return 1

def could_be_start( iRestOfHeader, iStartIndex, iNumStreams, iNumProxies, iNumSeenStreams):
    """Determine if position in given header can be a start of new record
    Start of list should have
      - a number less than # proxies followed by another number less than # proxies
        followed by a value that could be a valid string
      - or a 0 followed by a condition 1 or 2 BUT the number of times
        this condition is applied must be less than the number of streams
    """
    #print iRestOfHeader[iStartIndex]
    remainingSizeOfHeader = len(iRestOfHeader) - iStartIndex
    #every proxy needs at least 2 words   
    maxPossibleNumOfProxies = remainingSizeOfHeader / 2
    if iRestOfHeader[iStartIndex] > maxPossibleNumOfProxies:
       return 0
    
    if iNumStreams == 1:
       if iNumProxies > iRestOfHeader[iStartIndex]:
	  return 0
       
       if iRestOfHeader[iStartIndex+1] > iNumProxies:
	  return 0
       return could_be_string(iRestOfHeader, iStartIndex + 2)
    
    if 0 == iRestOfHeader[iStartIndex]:
       iNumSeenStreams = iNumSeenStreams + 1
       if iNumSeenStreams < iNumStreams:
	  return could_be_start( iRestOfHeader, iStartIndex+1, iNumStreams, iNumProxies, iNumSeenStreams)
       else:
	  return 0
    else:
       if iRestOfHeader[iStartIndex+1] > iNumProxies:
	  return 0
    return could_be_string(iRestOfHeader, iStartIndex + 2)

def find_probable_start( iRestOfHeader, iStartIndex, iNumStreams, iNumProxies):
    """Find a new start in pds file:
    Start of list should have
      - a number less than # proxies followed by another number less than # proxies
        followed by a value that could be a valid string
      - or a 0 followed by a condition 1 or 2 BUT the number of times
        this condition is applied must be less than the number of streams
    """
    endIndex = len(iRestOfHeader)
    startIndex = iStartIndex

    while endIndex > startIndex:
       presentIndex = startIndex
       #find a possible starting point
       for value in iRestOfHeader[startIndex: endIndex]:
	  remainingSizeOfHeader = len(iRestOfHeader) - presentIndex
	  #every proxy needs at least 2 words   
	  maxPossibleNumOfProxies = remainingSizeOfHeader / 2
	  if iRestOfHeader[presentIndex] <= maxPossibleNumOfProxies:
	     startIndex = presentIndex
	     break;
	  presentIndex = presentIndex + 1
       #print startIndex
       if presentIndex == endIndex + 1:
	  #didn't find a start
	  return endIndex
       if could_be_start( iRestOfHeader, startIndex, iNumStreams, iNumProxies, 0):
	  return startIndex
       startIndex = startIndex + 1
    return endIndex

ex_found_bad_word = "found bad word"

def read_tag( iCharArray, iIndex ):
    """Read usage/production tag from given array"""
    #find first occurance of 0
    firstZeroIndex= len(iCharArray)
    index = 0
    for character in iCharArray:
       if iIndex > index:
	  index = index + 1
	  continue
       if character == '\000' :
	  firstZeroIndex = index
	  break
       index = index +1
    return iCharArray[iIndex: firstZeroIndex].tostring()

def read_tags( iRestOfHeader, iIndex):
    """Read usage/production tags from given header"""
    if 0 == iRestOfHeader[iIndex]:
       return (iIndex+1, "", "")
    if not could_be_string(iRestOfHeader, iIndex):
       raise ex_found_bad_word, iRestOfHeader[iIndex:].tostring()

    # new stuff
    header=iRestOfHeader[iIndex:]
    if pdsSignature != PDSSIGNATURE: header.byteswap()
    charArray = array.array('c', header.tostring())
#   charArray = array.array('c', iRestOfHeader[iIndex:].tostring())
    usageTag = read_tag( charArray, 0)
    productionTag = read_tag( charArray, len(usageTag)+1 )
    #print usageTag + " " + productionTag
    lengthOfString = len(usageTag)+len(productionTag) + 2
    if 0 != lengthOfString%4:
       lengthOfString = lengthOfString + 4 - lengthOfString%4
    return ( iIndex + (lengthOfString )/4, usageTag, productionTag)


def find_proxies_in_streams( iRestOfHeader, iStreamNames, iProxyNames):
    """Find data proxies in a data streams"""
    #for consistency check, make sure all Proxies are used
    used_proxies = []
    for item in iProxyNames:
       used_proxies.append(0)
    returnValue = []
    for item in iStreamNames:
       returnValue.append([])
    maxProxyName = len(iProxyNames)

    startIndex  = 0
    continueSearch = 1
    ex_parse_error = "parse error"
    while continueSearch:
       try:
	  startIndex = find_probable_start(iRestOfHeader, startIndex, len(iStreamNames), len(iProxyNames))

	  #print "found start"
	  #print iRestOfHeader[int(startIndex):]
	  if startIndex >= len(iRestOfHeader):
	     raise "couldn't find start"
	  #print startIndex
	  index = startIndex
	  for proxyList in returnValue:
	     numberReadProxies = 0
	     if index >= len(iRestOfHeader):
		raise ex_parse_error, "went too far"
	     numberOfProxies = iRestOfHeader[index]
	     index = index + 1
	     #print "%s %i" % ("number of Proxies =", numberOfProxies) 
	     while numberReadProxies != numberOfProxies:
		numberReadProxies = numberReadProxies + 1
		#print index
		if index>=len(iRestOfHeader): break
		proxyType = iRestOfHeader[index]
		if proxyType > maxProxyName:
		   raise ex_parse_error, proxyType
		#print proxyType
		#now read strings
		index = index +1
		(index, usage, production) = read_tags( iRestOfHeader, index)
		#print index
		#print "my printout", iProxyNames
		#print "my_printout", len(iProxyNames)
		if len(iProxyNames) != int(proxyType) : proxyList.append( [iProxyNames[int(proxyType)],usage,production] )
		#print proxyList
	  if index +1 != len(iRestOfHeader):
	     raise ex_parse_error, "wrong length"
	  continueSearch = 0
       except (ex_parse_error, ex_found_bad_word):
	  #print sys.exc_type, sys.exc_value
	  #print iRestOfHeader[startIndex:]
	  startIndex = startIndex +1
	  #print startIndex
	  returnValue = []
	  for item in iStreamNames:
	     returnValue.append([])
    #print iRestOfHeader[startIndex:]
    return returnValue
    
def printProxies(pList):
    """Print proxies in pds file"""
    header=["ProxyName","UsageTag","ProductionTag"]
    # in first loop we mark largest strings for proxy/usage/production tags
    maxProxyLength=len(header[0])
    maxUsageTagLength=len(header[1])
    maxProductionTagLength=len(header[2])
    for p in pList:
        if len(p[0])>maxProxyLength: maxProxyLength=len(p[0])
        if len(p[1])>maxUsageTagLength: maxUsageTagLength=len(p[1])
        if len(p[2])>maxProductionTagLength: maxProductionTagLength=len(p[2])
    # printout in nice (aligned) format
    count = 0
    s = ""
    s0= ""
    s1= ""
    while len(s0)<(maxProxyLength-len(header[0])+1): s0=s0+" "
    while len(s1)<(maxUsageTagLength-len(header[1])+1): s1=s1+" "
    for p in pList:
        if not count:
	   s=s+header[0]+s0+header[1]+s1+header[2]+"\n"
	   
        if p[1]: usageTag=p[1]
	else:    usageTag="N/A"
	if p[2]: productionTag=p[2]
	else:    productionTag="N/A"

	ss0=""
	ss1=""
        while len(ss0)<(maxProxyLength-len(p[0])+1): ss0=ss0+" "
        while len(ss1)<(maxUsageTagLength-len(usageTag)+1): ss1=ss1+" "
	s=s+p[0]+ss0+usageTag+ss1+productionTag+"\n"
	count = count+1
    totLength=maxProxyLength+maxUsageTagLength+maxProductionTagLength+2
    ss = ""
    while len(ss)<totLength: ss=ss+"-"
    s = s+ss+"\n"+"Total number of proxies: %d\n"%count
    return s

def pdsDumpTime(file):
    """Dump unique id (UID) of pds file, in old notation it was called time"""
    fileDesc = open(file,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)
    global pdsSignature
    pdsSignature=headerHeader[0]>>8
    if pdsSignature != PDSSIGNATURE: headerHeader.byteswap()
    streamNames = name_list_from_file(fileDesc)
    shProxyNames = name_list_from_file(fileDesc)
    shNames = name_list_from_file(fileDesc)
  
    #grab the rest of the header
    restOfHeader = array.array('I')
    #the header size ignores the first 3 words in the event
    restOfHeader.fromfile(fileDesc, headerHeader[2] -fileDesc.tell()/4 +3)
    if pdsSignature != PDSSIGNATURE: restOfHeader.byteswap()
    if restOfHeader[-1] != headerHeader[2]:
       raise "header inconsistent"
    proxiesInStreams = find_proxies_in_streams( restOfHeader, streamNames, shProxyNames)

    #want to only look at stuff in event
    eventIndex = streamNames.index('event')

    #create a structure to hold our info
    eventProxies = proxiesInStreams[eventIndex]
    accumulatedData = []
    for proxy in eventProxies:
       accumulatedData.append([0,488888888,0])

    #start to read the rest of the file
    numEvents = 0
    timeStamp = 0
    while 1:
     try:
       endReason = ""
       recordHeader = array.array('I')
       recordHeader.fromfile(fileDesc,5)
       if pdsSignature != PDSSIGNATURE: recordHeader.byteswap()
       #print "time(64bit) "+str((recordHeader[4]<<32)|recordHeader[3])
       #print str((recordHeader[4]<<32)|recordHeader[3])
       timeStamp = str((recordHeader[4]<<32)|recordHeader[3])
       break
     except EOFError:
      print endReason
      break
    return timeStamp

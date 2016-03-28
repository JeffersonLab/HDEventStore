#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""Dump content of key (index) file. Also contain countEvents routine
for counting number of events present in key file."""
### UPDATE

import array, string, re, sys
import gen_util
from os_path_util import NullDevice

def keyFileHeaderReader(keyFileName):
    """Read key file header and return file offset to Records"""

    KEYSIGNATURE=2718281                      
    SIGNATURE = KEYSIGNATURE
    keyFile   = open(keyFileName,'rb')
    headerHeader = array.array('I')
    headerHeader.fromfile(keyFile, 3)

    # to handle endianess, read keySignature
    keySignature = headerHeader[0]>>8
    needToSwap=0
    if keySignature != SIGNATURE:
       headerHeader.byteswap()
       needToSwap=1

    position = keyFile.tell()
    keyFile.close()
    return [position,needToSwap,headerHeader[1],headerHeader[2]]
    
def keyFileParser(keyFileName):
    """Parse key file and read back all syncValues"""

    keyFile       = open(keyFileName,'rb')
    keyFileHeader = keyFileHeaderReader(keyFileName)
    if not keyFileHeader: return 0 # error
    keyFile.seek(keyFileHeader[0])
    needToSwap  = keyFileHeader[1]

    svList = []
    while 1:
       try:
	  record = array.array('I')
	  record.fromfile(keyFile,6)
	  if needToSwap: record.byteswap()
	  syncValue = (record[0],record[1],record[2])
	  svList.append(syncValue)
       except EOFError:
	  break
    keyFile.close()
    svList.sort()
    return svList

    
def countEvents(keyFileName,quiet=1,whereToWrite="null"):
    """Event counter method, it counts a number of events presented in 
    a key file"""

    if whereToWrite=="null":
       # Redirect the standard file descriptors to /dev/null.
       sys.stdin.close()
       sys.stdout = NullDevice()
       sys.stderr = NullDevice()
    try:
       position,needToSwap,fileUid,nSyncValues=keyFileHeaderReader(keyFileName)
    except:
       print "Fail to parse header of",keyFileName
       gen_util.printExcept()
       return 0 # error
    keyFile       = open(keyFileName,'rb')
    keyFile.seek(position)
    if needToSwap:
       print "File was produced on another endian machine, byte swapping is enabled"

    firstSV      = ""
    lastSV       = ""
    nSV          = 0
    svRecordList = []
    maxIdx       =-1
    magicNumber  = 4294967295 # 2*32-1

    while 1:
       try:
           sv = array.array('I')
           sv.fromfile(keyFile,6)
           run = sv[0]
           evt = sv[1]
           uid = sv[2]
           nSV+=1
           print "%d/%d/%d"%(run,evt,uid)
           if not len(firstSV):
               firstSV="%d/%d/%d"%(run,evt,uid)
               lastSV="%d/%d/%d"%(run,evt,uid)
       except EOFError:
           break
    keyFile.close()
    if nSV == 0:
        raise "no events found"

    # Redirect the standard file descriptors to default I/O
    sys.stdin  = sys.__stdin__
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__

    return nSV
    
    
def dump(keyFileName,verbose=1,whereToWrite="std"):
    """Dump content of a key file. The output can be redirected to /dev/null
    or stdout."""

    if whereToWrite=="null":
       # Redirect the standard file descriptors to /dev/null.
       sys.stdin.close()
       sys.stdout = NullDevice()
       sys.stderr = NullDevice()
    try:
       position,needToSwap,fileUid,nSyncValues=keyFileHeaderReader(keyFileName)
    except:
       print "Fail to parse header of",keyFileName
       gen_util.printExcept()
       return 0 # error
    keyFile       = open(keyFileName,'rb')
    keyFile.seek(position)
    if needToSwap:
       print "File was produced on another endian machine, byte swapping is enabled"

    firstSV      = ""
    lastSV       = ""
    nSV          = 0
    svRecordList = []
    maxIdx       =-1
    magicNumber  = 4294967295 # 2*32-1
    while 1:
       try:
           sv = array.array('I')
           sv.fromfile(keyFile,6)
           run = sv[0]
           evt = sv[1]
           uid = sv[2]
           svRecordList.append( ( run,evt,uid ) )

           nSV+=1
           if not len(firstSV):
               firstSV="%d/%d/%d"%(run,evt,uid)
           lastSV="%d/%d/%d"%(run,evt,uid)
           if verbose:
               print "********"
               print "SyncValue: "+str(run)+" "+str(evt)+" "+str(uid)
       except EOFError:
           break
    keyFile.close()
    if nSV == 0:
        raise "no events found"

    print "First sync value      :",firstSV
    print " Last sync value      :",lastSV
    print "Number of syncValues  :",nSV
    print "Number of sv in header:",nSyncValues
    print

    if nSyncValues != nSV:
        raise Exception("nSyncValues in header doesn't match number found")
    
    # Redirect the standard file descriptors to default I/O
    sys.stdin  = sys.__stdin__
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    return svRecordList

def decodeKeyRecord(keyFile,needToSwap,nRecordTypes):
    """Decode a record in key file at current position of key file"""

    record = array.array('I')
    record.fromfile(keyFile,6)
    if needToSwap: record.byteswap()
    syncValue = (record[0],record[1],record[2])
    recordIndex = (record[4]<<32) | record[5]
    return (syncValue,recordIndex)


###################################################################
# DANGER: this is not correct!
###################################################################
def stripKeyFile(keyFileName,iNewFileName,newFileId,skimKeyFileName):
    """From given key file name, new file id and fake sv list recreate key file.
    Return new file name and write out the file."""

    print "stripKeyFile called - look at this code!"
    sys.exit(1)

    newFileName = string.split(iNewFileName,"-esdb")[0]+"-esdb.ikey"
    pos,needToSwap,nRecordNames,sNames,nSV=keyFileHeaderReader(keyFileName)
    origKeyFile = open(keyFileName,'r')
    origKeyFile.seek(pos)

    # read information from skimKeyFileName
    skimPos,skimSwap,skimRecords,skimStreamNames,skimNumberSV=keyFileHeaderReader(skimKeyFileName)
    skimKeyFile = open(skimKeyFileName,'r')
    skimKeyFile.seek(skimPos)
    
    streamNameString =""
    for name in sNames:
        streamNameString = streamNameString+name +"\0"
    streamNameString = streamNameString[:-1]
    while 0 != len(streamNameString) % 4:
        streamNameString = streamNameString + "\0"
    nameWords = array.array('I')
    nameWords.fromstring(streamNameString)

    nameList   = nameWords.tolist()
    number32BitWordsInNameList = len(nameList)
    nWordsInHeader = len(sNames)+number32BitWordsInNameList+1+1+1
    headerList = [2718281*256,newFileId,nWordsInHeader,len(sNames)]
    headerList+= [number32BitWordsInNameList]+nameList+[nSV,nWordsInHeader]
    keyFile    = open(newFileName,'w+b')
    keyHeader  = array.array('I')
    keyHeader.fromlist(headerList)
    keyHeader.tofile(keyFile)
    
    # loop over all sv's in original key file and write out those which are not in fakeSVList
    i       = 0
    fakeIdx = 0
    counter = 0
    skimSV,skimIdxList = decodeKeyRecord(skimKeyFile,skimSwap,skimRecords)
    print "orig",origKeyFile.name
    print "skim",skimKeyFile.name
    while i<nSV:
       if not skimSV: break
       sv,recIdxList = decodeKeyRecord(origKeyFile,needToSwap,nRecordNames)
       if not sv: break
       i+=1
       # we event number from parent key file is become larger then from skim, advance the skim
       if sv[1]>skimSV[1]:
          try:
             skimSV,skimIdxList = decodeKeyRecord(skimKeyFile,skimSwap,skimRecords)
          except:
             skimSV=()
             break
       if skimSV!=sv:
          continue
       else: # write out
          run = sv[0]
          evt = sv[1]
          lowerId, upperId = gen_util.lowerUpperBitsOfUID(sv[2])
          recordArray = array.array('I')
          recordHeader = [sv[0],sv[1],lowerId,upperId]+skimIdxList
          recordArray.fromlist(recordHeader)
          recordArray.tofile(keyFile)
          counter+=1
          # read one more event from skim file
          try:
             skimSV,skimIdxList = decodeKeyRecord(skimKeyFile,skimSwap,skimRecords)
          except:
             skimSV=()
    # seek to position where total SV's written and fix it to new number of SV's
    keyFile.seek(pos-2*4)
    pos = keyFile.tell()
    nSVHeader = array.array('I')
    nSVHeader.fromfile(keyFile,1)
    if nSVHeader[0]!=nSV:
       print "nSVHeader[0]",nSVHeader[0],nSV
       print "Miss position of nSyncValues in a file"
       return ""
    keyFile.seek(pos)
    nSVHeader[0]=counter
    nSVHeader.tofile(keyFile)
    keyFile.close()

    return newFileName

if __name__ == "__main__":
    svRecordList = dump(sys.argv[1],1,"std")
    for item in svRecordList:
        print item
    print countEvents(sys.argv[1])
#     print recreateKeyFile(sys.argv[1],1000,[(208088,1,1),(208088,10,1)])


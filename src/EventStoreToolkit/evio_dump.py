#!/usr/bin/env python                                                                                                                           
# Author:  Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
"""HDDM REST file dump tools"""

import array, string, re, sys
import struct

import evio_utils

VERBOSE = False

class EVIOError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class EVIOEOF(Exception):
    pass

def readEVIOBlock(evioFile):
    """Return header and payload separately"""
    global VERBOSE
    # settings
    BUFFER_LIMIT = 5000000

    # the header is 8 words long
    header_in = evioFile.read(8*4)
    if len(header_in) != 8*4:
        raise EVIOError("Could not read in 8 word EVIO block header!")
    header = struct.unpack(">8I", header_in)

    block_length = header[0]
    swap_needed = (header[7]==0x0001dac0)
    
    if VERBOSE:
        print "Read in EVIO block!"
        print "  block length = " + str(header[0])
        print "  block number = " + str(header[1])
        print "  event number = " + str(header[3])
        print "  swap needed  = " + str(swap_needed)

    # check magic header word
    if(header[7]!=0xc0da0100 and header[7]!=0x0001dac0):
        raise EVIOError("Magic word invalid! = " + str(hex(header[7])) + "(file = %s)"%evioFile.name)

    # a block length of 8 (just a header) signals end of file
    if(block_length == 8):
        raise EVIOEOF
        
    if(block_length > BUFFER_LIMIT):
        raise EVIOError("ERROR: EVIO block length greater than allocation limit! (%d > %d words!)" % (block_length,BUFFER_LIMIT))

    # rewind over header and read in the whole buffer
    #evioFile.seek(-8*4, 1)
    #block_in = evioFile.read(4*block_length)
    #if(len(block_in) < 4*block_length):
    #    raise EVIOError("ERROR: Read in fewer words than requested! (%d > %d words!)" % (len(block_in)/4,block_length))
    #block = struct.unpack(str(block_length)+"I", block_in)

    # rewind over header and read in the beginning of the buffer
    # we are only really interested in getting the run and event numbers
    # so we only need the first part of the event
    evioFile.seek(-8*4, 1)
    CHUNKSIZE = 64  # max number of words to read in
    tmp_blocksize = block_length
    if tmp_blocksize > CHUNKSIZE:
        tmp_blocksize = CHUNKSIZE
    block_in = evioFile.read(4*tmp_blocksize)
    if(len(block_in) < 4*tmp_blocksize):
        raise EVIOError("ERROR: Read in fewer words than requested! (%d > %d words!)" % (len(block_in)/4,tmp_blocksize))
    block = struct.unpack(str(tmp_blocksize)+"I", block_in)
    evioFile.seek((block_length-tmp_blocksize)*4, 1)

    #if(block_length == 8)  ## EOF -- do something here

    if VERBOSE:
        print "read in block of length = " + str(block_length)

    return (header,block,block_length)

def GetRunEventNumbers(event, recordLength):
    #for i in xrange(recordLength):
    for i in xrange(len(event)):
        w = event[i]
        # Physics Event Header bits that should be set
        if( (w & 0x001050FF) != 0x001050FF ):
            continue
        # Physics Event Header bits that should not be set
        if( (w & 0x000F0E00) != 0x00000000 ):
            continue
        # Jump 2 words to Trigger bank
        w = event[i+2];
        # Built Trigger Bank bits that should be set
        if( (w & 0x002020FF) != 0x002020FF ):
            continue
        # First bank in Trigger bank should be 64bit int type
        w = event[i+3];
        if( (w & 0x00000A00) != 0x00000A00 ):
            continue
                        
        eventNumber = evio_utils.EVIO_SWAP64( (long(event[i+5])<<32) | long(event[i+4]) )
        timestamp = evio_utils.EVIO_SWAP64( (long(event[i+7])<<32) | long(event[i+6]) )
        runNumber = evio_utils.EVIO_SWAP64( (long(event[i+9])<<32) | long(event[i+8]) ) >> 32

        return (runNumber, eventNumber)

    # somehow we didn't find the event correctly?
    return (None,None)

def getEVIORecordInfo(evioFile):
    """Read information from one EVIO record"""
    fileOffset = long(evioFile.tell()/4)
    recordLength = 0
    runNumber = -1    
    eventNumber = -1
    uid = -1           ## dummy value for now

    # read in event
    (header, event, recordLength) = readEVIOBlock(evioFile)
        
    # extract run and event numbers from event
    (runNumber, eventNumber) = GetRunEventNumbers(event, recordLength)
    
    # explicitly clean up, just in case
    del header
    del event

    # return values
    if runNumber is not None:
        syncValue = (runNumber, eventNumber, uid)
        return (syncValue, recordLength, fileOffset)
    else:
        return (None, recordLength, fileOffset)

def getHDDMRecord(evioFile):
    return getHDDMRecordInfo(evioFile)



class EVIOFileReader:
    """Base class to read EVIO file"""
    def __init__(self,fileName):
        self.evioFile = open(fileName,"rb")
        self.fileIndex = int(0)    ## keeps track of which word we are at in the file
        self.run = evio_utils.GetRunNumberFromFilename(fileName)
        self.uid = long(1)
        self.nevents = 0     # number of processed events
        # if it's MC, the uid = file number, extract it from file name of form dana_rest_RRRRR_FFFFFFF.hddm        
        # otherwise, assume uid = 1 (i.e. data)
        #matchObj = re.match( r'dana_rest_\d\d\d\d\d_(\d\d\d\d\d\d\d).hddm', self.hddmFile.getFilename(), re.M|re.I)
        #if matchObj:
        #    self.uid = int(matchObj.group(1))        
    def __del__(self):
        self.evioFile.close()
    def close(self):
        self.evioFile.close()

    def readRecordInfo(self):
        """Return current record information, we use L{getEVIOEventInfo} method"""
        syncValue = None
        # skip over non-physics events
        while syncValue is None:
            try:
                (syncValue,recordLength,fileOffset) = getEVIORecordInfo(self.evioFile)
            except EVIOEOF:
                return (None,None)
            self.fileIndex = self.fileIndex + recordLength
        #return (syncValue, self.fileIndex)

        # some useful output...
        self.nevents += 1
        if( (self.nevents%10000) == 0 ):
            print "processed %d events ..." % (self.nevents)

        # for now, overwrite the uid value
        return ( (syncValue[0],syncValue[1],self.uid), fileOffset )
    def readRecord(self):
        """Read one record in pds file, we use L{readHDDMRecord} method"""
        return self.readRecordInfo()
    def backToFirstRecord(self):
        """Move internal cursors back to the first record in the file
        @rtype: none                                                                                                 
        @return: seek to the first record in a file"""
        # do we need to skip a file header?
        self.evioFile.seek(0,0)
        self.fileIndex = int(0)
        self.nevents = 0
    def readLastRecordInfo(self):
        """Return last record in file. Record info is retrieved by L{getEVIORecordInfo} method"""
        ## lazy implemention - run through the whole file and save the last block
        self.backToFirstRecord()
        numRecords = 0
        lastRecord = []
        while 1:
            newRecord = self.readRecord()
            if newRecord[0] is None:
                break
            lastRecord = newRecord

        return lastRecord


if __name__ == "__main__":
    reader = EVIOFileReader(sys.argv[1])
    while 1:
        (syncValue, index) = reader.readRecordInfo()
        if syncValue is not None and syncValue[0]:
            print syncValue, index
        else:
            break

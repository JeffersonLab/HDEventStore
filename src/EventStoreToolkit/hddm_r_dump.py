#!/usr/bin/env python                                                                                                                           
# Author:  Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
"""HDDM REST file dump tools"""

import array, string, re, sys
import pyhddm_r

# I don't know if we need this function
#def dump(fileName,verbose=0):

#this might be useful some day
#def decodeVersionInfo(fileName):
#    """Decode VersionInfo from beginrun record.                                                                      
#    VersionInfo consists of:                                                                                         
#       - softwareRelease : string                                                                                    
#       - specificVersionName : string                                                                                
#       - configurationHash : string                                                                                  
#       - ordinal : unsigned int no packing                                                                           
#       - ancestors : container of string                                                                             
#    So, it always grows, e.g. post-p2 file will contains two VersionInfo's, one                                      
#    for itself and one for it's parent. So, the underlying algorithm creates                                         
#    a list of VersionInfo's in the following format:                                                                 
#    [(childTag,[softRel,svName,hash,id,parent1,parent2,...]),(parentTag,[...])]                                      
#    This method returns (svName,[listOfParents]) tuple                                                               
#    """

def getHDDMRecordInfo(hddmFile):
    """Read information from one REST record
    @type hddmFile: file descriptor
    @param hddmFile: hddm file descriptor
    @rtype: tuple
    @return: syncValue
    """
    if not hddmFile.read():
        return None
    else:
        # if it's MC, the uid = file number, extract it from file name of form dana_rest_RRRRR_FFFFFFF.hddm        
        # otherwise, assume uid = 1 (i.e. data)
        uid = int(1)
        #matchObj = re.match( r'dana_rest_\d\d\d\d\d_(\d\d\d\d\d\d\d).hddm', hddmFile.getFilename(), re.M|re.I)
        #if matchObj:
        #    uid = long(matchObj.group(1))
        # form a syncValue                                                                           
        
        # if this isn't a physics event, then return a null sync value
        if hddmFile.getRunNumber()==0 and hddmFile.getEventNumber()==0:
            return None
                    
        syncValue=(hddmFile.getRunNumber(),hddmFile.getEventNumber(),uid)
        return syncValue

def getHDDMRecord(hddmFile):
    return getHDDMRecordInfo(hddmFile)



class HDDMFileReader:
    """Base class to read REST file"""
    ### NOTE: should add some sort of caching here...
    def __init__(self,fileName):
        self.fileName = fileName
        self.hddmFile = pyhddm_r.hddm_istream_proxy(fileName)
        self.fileIndex = int(0)    ## keeps track of which event we are at in the file
        self.uid = int(1)
        # if it's MC, the uid = file number, extract it from file name of form dana_rest_RRRRR_FFFFFFF.hddm        
        # otherwise, assume uid = 1 (i.e. data)
        matchObj = re.match( r'dana_rest_\d\d\d\d\d_(\d\d\d\d\d\d\d).hddm', self.hddmFile.getFilename(), re.M|re.I)
        if matchObj:
            self.uid = int(matchObj.group(1))        
    def __del__(self):
        self.hddmFile.close()

    def readRecordInfo(self):
        """Return current record information, we use L{getHDDMEventInfo} method"""
        if not self.hddmFile:
            return ( None, None )
        if self.hddmFile.eof():
            return ( None, None )

        syncValue = None
        # skip over non-physics events
        while syncValue is None:
            syncValue = getHDDMRecordInfo(self.hddmFile)
            self.fileIndex = self.fileIndex + 1

        # for now, overwrite the uid value
        if syncValue is None:
            return ( None, None )
        else:
            return ( (syncValue[0],syncValue[1],self.uid), self.fileIndex )
    def readRecord(self):
        """Read one record in pds file, we use L{readHDDMRecord} method"""
        return self.readRecordInfo()
    def backToFirstRecord(self):
        """HDDM file descriptor of the first record in a file.                                                        
        @rtype: none                                                                                                 
        @return: seek to the first record in a file"""
        ## the HDDM interface is a strean-based interface, so we just close the file and reopen
        ## then skip to the last one and return it                                                                                              
        self.hddmFile.reset()
        #del self.hddmFile
        #self.hddmFile = pyhddm_r.hddm_istream_proxy(self.fileName)
        self.fileIndex = int(0)
    def readLastRecordInfo(self):
        """Return last record in pds file. Record info is retrieved by L{getPDSRecordInfo} method"""
        ## again, since this is a stream-based interface, we read the whole file to the number of records
        ## then skip to the last one and return it
        self.hddmFile.reset()
        #del self.hddmFile
        #self.hddmFile = pyhddm_r.hddm_istream_proxy(self.fileName)
        self.fileIndex = int(0)
        #self.hddmFile.close()
        #self.hddmFile = hddm_r.hddm_istream_proxy(fileName)
        numRecords = 0
        lastRecord = (None,None)
        #while self.hddmFile.read():
        #    numRecords = numRecords + 1
        while 1:
            newRecord = self.readRecord()
            if newRecord and newRecord[0]:
                lastRecord = newRecord
            if self.hddmFile.eof():
                break
            #if not newRecord[0]:
            #    break

        return lastRecord
        # done with the counting, let's get the last one
        #self.hddmFile.reset()
        #self.hddmFile.close()
        #self.hddmFile = hddm_r.hddm_istream_proxy(fileName)
        #self.hddmFile.skip(numRecords-1)
        #return self.readRecord()        

if __name__ == "__main__":
    reader = HDDMFileReader(sys.argv[1])
    while 1:
        (syncValue, index) = reader.readRecordInfo()
        if syncValue[0]:
            print syncValue, index
        else:
            break

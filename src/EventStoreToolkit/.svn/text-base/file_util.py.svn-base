#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""Set of high-level functions to build key/location files
regardless from input data format. All EventStore file types are
determined by fileType method."""

import os,sys,string,array
# import auxilary modules for key/location files
import os_path_util, gen_util
import hddm_r_dump, hddm_r_reader
#import hddm_s_dump, hddm_s_reader
import evio_dump, evio_reader
#import hddm_reader ##, lhddm_dump 
#import build_key_from_hddm 
##import build_hddm_location 
import build_key_from_hddm_r #, build_key_from_hddm_s
import build_key_from_evio

def fileParser(fileName,what=""):
    """A high-level method to parse data files in EventStore. Based on a file type it
    propagates a request to appropriate module."""
    type = fileType(fileName)
    if type == "rest": 
       content = hddm_r_reader.hddmParser(fileName,what)
    #elif type == "mc": 
    #   content = hddm_s_reader.hddmParser(fileName,what)
    elif type == "evio": 
       content = evio_reader.evioParser(fileName,what)
    else:
       print "Format of the %s is not recognized (format=%s)"%(fileName,type)
       sys.exit(1)
    return content

def runParser(fileName):
    """A high-level method to parse files in EventStore and return run content."""
    type = fileType(fileName)
    if type == "rest": 
       content = hddm_r_reader.hddmRunParser(fileName)
    #elif type == "mc": 
    #   content = hddm_s_reader.hddmRunParser(fileName)
    elif type == "evio":
       content = evio_reader.evioRunParser(fileName)
    else:
       print "Format of the %s is not recognized (format=%s)"%(fileName,type)
       sys.exit(1)
    return content

def locationFileParser(fileName):
    """A high-level method to parse location files in EventStore"""
    print "Location files are not currently supported!"%fileIn
    sys.exit(1)
    #type = fileType(fileName)
    #if type == "lhddm":
    #   content = lhddm_dump.locationFileParser(fileName)
    #else:
    #   print "Format of the %s is not recognized"%fileName
    #   sys.exit(1)
    #return content

def changeFileIdsInLocFile(locFileName,fileIdList):
    """A high-level method to change fileIds in location file to given list"""
    type = fileType(locFileName)
    if type == "lhddm":
       build_hddm_location.changeFileIdsInLocFile(locFileName,fileIdList)
    else:
       print "Format of the %s is not recognized (format=%s)"%(locFileName,type)
       sys.exit(1)
    return
	
def getFileIds(locFileName):
    """A high-level method to get a list of fileIds from location file"""
    type = fileType(locFileName)
    file2IdList = []
#    if type == "lhddm":
#       file2IdList = lhddm_dump.getFileIds(locFileName)
#    else:
    print "Format of the %s is not recognized (format=%s)"%(locFileName,type)
    sys.exit(1)
	
    # form 64-bit fileIds out of two 32-bit numbers
    fileIdList= []
    idx = 0
    while idx<len(file2IdList):
	  lower = file2IdList[idx]
	  idx  += 1
	  upper = file2IdList[idx]
	  idx  += 1
	  fileIdList.append(gen_util.form64BitNumber(lower,upper))
    return fileIdList
	
#def getProxies(fileName):
#    """A high-level method to get data (proxies) from given file"""
#    type = fileType(fileName)
#    if type == "lhddm":
#       content = lhddm_dump.getProxies(fileName)
#    elif type == "lbin":
#       content = lbin_dump.getProxies(fileName)
#    else:
#       print "Format of the %s is not recognized"%fileName
#       sys.exit(1)
#    return content

def build_key(fileIn,fileOut,oFileID):
    """A high-level method to build key files in EventStore"""
    type = fileType(fileIn)
    if type=="rest":
       return build_key_from_hddm_r.build_key(fileIn,fileOut,oFileID)
    elif type=="evio":
       return build_key_from_evio.build_key(fileIn,fileOut,oFileID)
    else:
       print "Format of the %s is not recognized"%fileIn
       sys.exit(1)

def build_location(fileIn,fileID,fileOut,allList=[]):
    """A high-level method to build location files in EventStore"""
    print "Location files are not currently supported!"%fileIn
    sys.exit(1)
#    type = fileType(fileIn)
#    if type=="hddm":
#       return build_hddm_location.build_hddm_location_r(fileIn,fileID,fileOut,allList)
#    else:
#       print "Format of the %s is not recognized"%fileIn
#       sys.exit(1)
	

def fileType(fileName):   
    """A high-level method to determine file type. For ES files, it uses file signature:
       - KEYSIGNATURE=2718281
       - LOCSIGNATURE=2951413,
    For HDDM files
    It is endian compliant."""
    KEYSIGNATURE=2718281 # magic number for Key file
    LOCSIGNATURE=2951413 # magic number for Location file
    SIGNATURE=0          # signature we read from input file
    SWAPPEDSIGNATURE=0   # signature we read from input file (w/ swapped bytes)

    #print "CHECKING FILE TYPE"

    if os_path_util.isFile(fileName):
       fileDesc = open(fileName,'rb')
       fileDesc.seek(0)
    else:
       print "file_util: file %s not found"%fileName
       sys.exit(1)

    ## test if file is IDXA format first
    fileDesc.seek(0)
    if string.find(fileDesc.readline(),"IDXA")!=-1:
        fileDesc.close()
        return "idxa"

    ## check to see if file begins with "<HDDM"
    ## if so, use file "class" to determine type
    fileDesc.seek(0)
    snippet = fileDesc.read(5)
    #print "first snippet check = %s"%snippet
    if( snippet == "<?xml" ):
        fileDesc.seek(-5,1)
        xmlspec = fileDesc.readline()
        #print "XML spec = %s"%xmlspec
        snippet = fileDesc.read(5)
    #print "first snippet check = %s"%snippet
    if( snippet == "<HDDM" ):
        fileDesc.seek(-5,1)
        hddmspec = fileDesc.readline()
        #print "HDDM spec = %s"%hddmspec
        entries = hddmspec.split()
        #print "entries = %s"%str(entries)
        (key,value) = entries[1].split('=')
        fileDesc.close()
        #print "key = %s  value = %s"%(key,value)
        if key == "class":
            #print "FOUND CLASS"
            #print "TYPE = %s"%value
            if value == "\"r\"":
                #print "REST FILE"
                return "rest"
            elif value == "\"s\"":
                return "mc"
        return "hddm"   ## if all else fails at least we know it's some type of HDDM file?

    ## See if the file is some EventStore file
    fileDesc.seek(0)
    headerHeader = array.array('I')
    headerHeader.fromfile(fileDesc, 3)

    # read file signature
    SIGNATURE = headerHeader[0]>>8

    # look if we match any of signatures
    what = ""
    if SIGNATURE == KEYSIGNATURE: what="ikey"
    #elif SIGNATURE == LOCSIGNATURE: what="lpds"

    # if nothing matches, swap bytes and check again
    headerHeader.byteswap()
    SWAPPEDSIGNATURE = headerHeader[0]>>8

    if SWAPPEDSIGNATURE == KEYSIGNATURE: what="ikey"
    #elif SWAPPEDSIGNATURE == LOCSIGNATURE: what="lpds"

       
    # if file still not determined, try its extension
    if not what:
       what = string.split(fileName,".")[-1]
    fileDesc.close()

    return what
    

#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""Make a skim file out of input PDS and IDXA files"""

import os, sys, string, array
import idxa_reader, pds_dump, file_util

if __name__ == "__main__":
   usage = "\nUsage: makeSkim pdsFileName idxaFileName"
   usage+= "\n       Examples: makeSkim my.pds my.idxa"
   usage+= "\n                 will produce my_skim.pds file"
   if len(sys.argv)!=3:
      print usage
      sys.exit(1)
   pdsFile   = sys.argv[1]
   if file_util.fileType(pdsFile)!="pds":
      print "File %s is not a PDS file"%pdsFile
      print usage
      sys.exit(1)
   idxaFile  = sys.argv[2]
   if file_util.fileType(idxaFile)!="idxa":
      print "File %s is not a IDXA file"%idxaFile
      print usage
      sys.exit(1)
   skimName  = string.split(pdsFile,".")[0]+"_skim.pds"
   fileDesc  = open(skimName,'w+b')
   pdsHeader = pds_dump.getPDSHeader(pdsFile)
   pdsHeader.tofile(fileDesc)

   # parse IDXA file and loop over its syncValues
   idxaReader= idxa_reader.IDXAFileReader(idxaFile)
   pdsReader = pds_dump.PDSFileReader(pdsFile)
   for svRecord in idxaReader.svStreamList:
       sv    = svRecord[0]
       recIdx= svRecord[1]
       pos   = pdsReader.pdsFile.tell()
       recordInfo = pdsReader.readRecordInfo()
       while 1:
           if sv==recordInfo[0] and recIdx==recordInfo[2]:
              print "Write",recordInfo
              pdsReader.setPosition(pos)
              pdsRecord = pdsReader.readRecord()
              if pdsRecord:
                 fileOffsetList= pdsRecord[0]
                 recordHeader  = pdsRecord[1]
                 recordData    = pdsRecord[2]
                 recordHeader.tofile(fileDesc)
                 recordData.tofile(fileDesc)
                 break
           pos = pdsReader.pdsFile.tell()
           recordInfo = pdsReader.readRecordInfo()
           if not recordInfo[0]: break

#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""IDXA file maker from provided REST file"""

import os, sys, string, array
import idxa_reader, hddm_r_dump, file_util

if __name__ == "__main__":
   usage = "\nUsage: make_REST_IDXA hddmFileName <idxaFileName>"
   usage+= "\n       Examples: make_REST_IDXA my.hddm"
   usage+= "\n                 will produce my.idxa file"
   usage+= "\n       Examples: make_REST_IDXA my.hddm o.idxa"
   if len(sys.argv)==1:
      print usage
      sys.exit(1)
   hddmFile  = sys.argv[1]
   #if file_util.fileType(hddmFile)!="hddm":   
   print "FILE TYPE = %s" % (file_util.fileType(hddmFile))
   #if file_util.fileType(hddmFile)!="rest":   
   if file_util.fileType(hddmFile)!="hddm":   
      print "File %s is not a HDDM file"%hddmFile
      print usage
      sys.exit(1)
   if len(sys.argv)==3:
      idxaFile = sys.argv[2]
      if file_util.fileType(idxaFile)!="idxa":
         print "File %s is not a IDXA file"%idxaFile
         print usage
         sys.exit(1)
   else:
      idxaFile = string.split(hddmFile,".")[0]+".idxa"
   
   hddmReader= hddm_r_dump.HDDMFileReader(hddmFile)
   header   = ['IDXA']

   fileDesc  = open(idxaFile,'w')
   for item in header:
       fileDesc.write(item+"\n")
   # read all hddm records
   while 1:
      (syncValue, recIdx) = hddmReader.readRecordInfo()
      ## extract event info here
      run = syncValue[0]
      event = syncValue[1]
      uid = syncValue[2]
      if run:
         fileDesc.write("%s %s %s\n"%(run, event, uid))
      else:
         break
   fileDesc.close()

#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2008
#

import os, sys, string, shutil
import file_util

#
# main
#
if __name__ == "__main__":
   if len(sys.argv)<4:
      print "Usage: ESChangeLocIds <locFileName> <newLocFileName> <newFileId1> <newFileId2>"
      sys.exit(1)
   fileName = sys.argv[1]
   newFileName = sys.argv[2]
   try:
      newFileIdList = [long(id) for id in sys.argv[3:]]
   except:
      raise "Cannot convert fileIds",str(sys.argv[3:]),"to integers"
   print "Change fileIds:"
   print fileName
   fileIdList = file_util.getFileIds(fileName)
   print fileIdList
   print
   print newFileName
   print newFileIdList
   if len(fileIdList)!=len(newFileIdList):
      raise "Old/new fileIds have different size"
   if fileName!=newFileName:
      print "Copy",fileName,"to",newFileName
      shutil.copy(fileName,newFileName)
      os.chmod(newFileName,0644)
   file_util.changeFileIdsInLocFile(newFileName,newFileIdList)


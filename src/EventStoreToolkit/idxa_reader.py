#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004, 2005
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""Base class to read IDXA files"""

import sys, string, re

class IDXAFileReader:
   """Base class to read information from IDXA files."""
   def __init__(self,fileName):
       """Base class to read information from IDXA files.
       You may access information from data members: eventList
       @type fileName: string
       @param fileName: name of the file
       @rtype: none
       @return: none
       """
       idxaFile = open(fileName,'r')
       lines    = idxaFile.readlines()
       self.eventList    = []
       self.runList      = []
       self.runUidList   = []
       self.svList       = []
       idx = 0
       while idx<len(lines):
           #line = string.replace(lines[idx],"\n","")
           line = lines[idx].rstrip()
           #print "DEBUG: " + line
           if idx==0 and string.find(line,"IDXA")==-1:
              # make sure that the file starts with the "magic string"
              print "Input file is not an index file!"
              return
           elif idx==0:
              idx+=1
              continue
           #line = string.replace(lines[idx],"\n","")
           sv = string.split(line," ")
           run = int(sv[0])
           event = int(sv[1])
           uid = int(sv[2])
           self.eventList.append( (run,event) )
           self.svList.append( ((run,event), uid) )
           if run not in self.runList:
              self.runList.append(run)
              self.runUidList.append( (run,uid) )
           idx+=1
       idxaFile.close()
   def getSVList(self):
       """Return a list of sync. values read it from IDXA file.                                                      
       @rtype: list                                                                                                  
       @return: list of sync. values read it from IDXA file                                                          
       """
       return self.svList
   def getRunUidList(self):
       """Return a list of run,uid pairs read it from IDXA file.                                                     
       @rtype: list                                                                                                  
       @return: list of (run,uid) pairs read it from IDXA file                                                       
       """
       return self.runUidList
   def getEventList(self):
       """Return a list of (run,event) pairs read it from IDXA file.
       @rtype: list
       @return: list of (run,event) pairs read in from IDXA file
       """
       return self.eventList
   def getRunList(self):
       """Return a list of runs read it from IDXA file.
       @rtype: list
       @return: list of run numbers read in from IDXA file
       """
       return self.runUidList

if __name__ == "__main__":
   reader = IDXAFileReader(sys.argv[1])
   print reader.svList
   #print reader.eventList

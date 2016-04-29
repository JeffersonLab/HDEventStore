#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""A general wrapper over format dependent dump routines to print content
of supported files in EventStore, e.g. hddm, key, location
"""

import sys
import subprocess


def fileContent(args):
    """fileContent is a wrapper over format dependent dump routines
    It dump content of all supported file formats in EventStore."""

    import os,sys,string,array
    import file_util, key_dump

    usage = """ESFileContent prints the content of data(hddm)/key/location files.

    Usage: ESFileContent [ -v ] <file>
	   Use -v option for verbose output
    """

    if len(args) == 1 or args[1] == "-help" or args[1] == "--help":
       print usage
       sys.exit()

    verbose=0	     # verbose flag

    if len(args)==3:
       if args[1]=="-v": verbose=1
       fileName = args[2]
    elif len(args)==2:
       fileName = args[1]
    else:
       print usage
       sys.exit()

    if os.path.isfile(fileName): 
       fileDesc = open(fileName,'rb')
    else:
       print "File %s not found"%fileName
       print usage
       sys.exit()

    what = file_util.fileType(fileName)

    if what == "hddm":
       print "Found hddm file",fileName,"\n"
       cmd = "hddm-xml " + fileName
       proc = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE)
       for line in proc.stdout:
           print line.rstrip()
    elif what == "ikey":
       print "Found key file",fileName,"\n"
       key_dump.dump(fileName,verbose)
    #elif what == "lhddm":
    #   print "Found HDDM location file",fileName,"\n"
    #   lhddm_dump.dump(fileName,verbose)
    else:
       print "File format is not recognized\n"

#
# main
#
if __name__ == "__main__":
    fileContent(sys.argv)

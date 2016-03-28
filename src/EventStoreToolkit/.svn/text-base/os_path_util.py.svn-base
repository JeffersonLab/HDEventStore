#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2005
#
"""Set of high-level utils to manipulate with arbitrary files"""

import os, string

class NullDevice:
    """Redirect stdout to /dev/null"""
    def write(self, s):
        pass

def isFile(fileName,verbose=1):
    """Check if given fileName is a file. If it's a link print out a file name
    it points to"""
    if os.path.isfile(fileName): 
       return 1
    if os.path.islink(fileName):
       trueFile = os.readlink(fileName)
       if os.path.isfile(trueFile):
          if verbose:
	     print "os_path_util.isFile found: %s -> %s"%(fileName,trueFile)
          return 1
    return 0

def formAbsolutePath(file):
    """Form absolute path to given file"""
    if not len(file): return file
    if file[0]=="/": return file
    path = os.path.normpath(os.getcwd())+"/"
    return path+file

def checkPermission(dir):
    """Check permission to directory"""
    return os.access(dir,os.W_OK)

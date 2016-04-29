#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""A set of usefull utilities independent from EventStore"""

import os, sys, string, time

def dayAhead():
    """Form a day ahead in the YYYYMMDD format. To form such day we ask
    for seconds since epoch time.time(), add one day 60*60*24, convert to
    tuple in UTC format and send it for formating to time.strftime:

    int( time.strftime( "%Y%m%d",time.gmtime(time.time()+60*60*24) ) )"""
    return int( time.strftime( "%Y%m%d",time.gmtime(time.time()+60*60*24) ) )
	
def lowerUpperBitsOfUID(uid):
    """Return lower and upper bits of 64-bit number"""
    # read uid and check if we need to initialize lower(upper) bits
    lowerMostId = long(uid) & 0x00000000FFFFFFFFL # lowerMost 32 bit
    upperMostId = long(uid) >> 32                 # upperMost 32 bit
    return (lowerMostId,upperMostId)

def form64BitNumber(lower,upper):
    """Form a 64-bit number from two 32-bit ones"""
    number = (upper<<32) | lower
    return number

def changeFileName(fileName,fromField,toField):
    """Change portion of file name from 'fromField' to 'toField'. It uses
    string replace mechnism to make a change. Return original name if
    no 'fromField' and 'toField' provided."""
    if (not fromField and not toField) or fromField==toField:
       return fileName
    newFileName = string.replace(fileName,fromField,toField)
    return newFileName

def printExcept():
    """print exception type, value and traceback on stderr"""
    sys.excepthook(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2])
	
def printListElements(iList,msg=""):
    """Loop over elements in a list and print one in a time on stdout"""
    if msg:
       print
       print "### %s:"%msg
    for item in iList:
        print item 

def addToDict(iDict,key,value):
    """Add value as a list to the dictionary for given key. If dictionary contains such key, update
    its list with given value. Return dictionary itself."""
    if iDict.has_key(key):
       iList = iDict[key]+[value]
       iDict[key] = iList
    else:
       iDict[key]=[value]
    return iDict


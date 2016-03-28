#! /usr/bin/env python
#
# []
#
# Author Sean Dobbs (s-dobbs@northwestern.edu), 2015
#


import os,sys
from os import listdir, system
from os.path import isfile, join
from optparse import OptionParser
import subprocess

VERBOSE = False

# read in files from the command line
for fname in sorted(sys.argv[1:]):
    # initialize some variables
    success = False
    run = -1

    # scrape information from the file
    with open(fname) as f:
        if VERBOSE:
            print "processing %s ..." % fname
        for line in f:
            if line[:14] == "DONE:SUCCEEDED":
                success = True
                if VERBOSE:
                    print "Injection was successful"
            if line[:10] == "RUN_NUMBER":
                run = int(line.strip()[11:])
                if VERBOSE:
                    print "Run number = %d"%run
            if line[:6] == "Log is":
                logfile = line.split()[2]
            if line[:9] == "files are":
                datafiles = line.split()[2:]
                if VERBOSE:
                    print "files = " + str(datafiles)
            if line[:5] == "ES is":
                esdir = line.split()[2]

        # report if we didn't succeed
        if success == False:
            print ""
            print "Failed injection for run %s"%run
            print ""
            print "Job output = %s"%fname
            if esdir:
                print "EventStore dir = %s"%esdir
                proc = subprocess.Popen("ls -lh %s"%esdir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
                output = proc.communicate()[0]
                print output
            if logfile:
                print "Log file = %s"%logfile
            print "Data files:"
            if datafiles:
                for datafile in datafiles:
                    proc = subprocess.Popen("ls -lh %s"%datafile, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
                    output = proc.communicate()[0].strip()
                    print output
            #print ""
                

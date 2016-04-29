#! /usr/bin/env python
#
# Build list of runs that have associated skim files
#
# Author Sean Dobbs (s-dobbs@northwestern.edu), 2015
#

import os,sys
from os import listdir, system
from os.path import isfile, join
from optparse import OptionParser

VERBOSE = False

MIN_RUN = -1
MAX_RUN = 10000000
parser = OptionParser(usage = "build_runlist.py [options]")
parser.add_option("-b","--min_run", dest="min_run",
                  help="Minimum run number to process")
parser.add_option("-e","--max_run", dest="max_run",
                  help="Maximum run number to process")
(options, args) = parser.parse_args(sys.argv)
if options.min_run:
    MIN_RUN = int(options.min_run)
if options.max_run:
    MAX_RUN = int(options.max_run)


# set up directories
BASEDIR_FORMAT = "/volatile/halld/offline_monitoring/%s/%s/"
RUNPERIOD = "RunPeriod-2014-10"
DATAREVISION = "ver12"

BASEDIR = BASEDIR_FORMAT % (RUNPERIOD,DATAREVISION)
BASEDIR_IDXA = join(BASEDIR,"idxa")


if VERBOSE:
    print "checking directory = %s" % BASEDIR_IDXA

# get list of directories, one run per directory
rundirs = [ d for d in listdir(BASEDIR_IDXA) if os.path.isdir(join(BASEDIR_IDXA,d)) ]

for rundir in sorted(rundirs):
    run = int(rundir)
    if run<MIN_RUN or run>MAX_RUN:
        continue
    print str(run)

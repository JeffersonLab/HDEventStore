#! /usr/bin/env python
#
# This script takes the output of the offline monitoring/skimming scripts and 
# creates the files needed for EventStore building/injection
#
# Author Sean Dobbs (s-dobbs@northwestern.edu), 2015
#

import os,sys
from os import listdir, system
from os.path import isfile, join
from optparse import OptionParser

VERBOSE = False
MKDIRMODE = "775"

# map file has format: "skimname::skimfile"
def parse_master_idxa_map(filename):
    skims = {}
    with open(filename) as f:
        for line in f:
            (skim_name,skim_file) = line.strip().split("::")
            skims[skim_name] = skim_file
    return skims


# set up directories
EVIO_SKIMS = [ "2track" ]
IDXA_DIR_FORMAT = "/volatile/halld/offline_monitoring/%s/%s/idxa"
BASEDIR_FORMAT = "/work/halld/EventStore/%s/%s"

RUNPERIOD = "RunPeriod-2014-10"
DATAREVISION = "ver12"

BASEDIR = BASEDIR_FORMAT % (RUNPERIOD,DATAREVISION)
IDXA_DIR = IDXA_DIR_FORMAT % (RUNPERIOD,DATAREVISION)

# some high level setup
# using work disk - lustre filesystems like volatile have problems with sqlite files
eventstore_base_dir = BASEDIR
system("mkdir -p -m %s %s" %(MKDIRMODE,eventstore_base_dir))
eventstore_files_dir = eventstore_base_dir + "/files"
#eventstore_evio_dbs_dir = eventstore_base_dir + "/evio_index"
#eventstore_rest_dbs_dir = eventstore_base_dir + "/rest_index"
system("mkdir -p -m %s %s"%(MKDIRMODE,eventstore_files_dir))
#system("mkdir -p " + eventstore_evio_dbs_dir)
#system("mkdir -p " + eventstore_rest_dbs_dir)

# allow running over a subset of runs
MIN_RUN = -1
MAX_RUN = 10000000
parser = OptionParser(usage = "build_eventstore_inputs.py [options]")
parser.add_option("-b","--min_run", dest="min_run",
                  help="Minimum run number to process")
parser.add_option("-e","--max_run", dest="max_run",
                  help="Maximum run number to process")
(options, args) = parser.parse_args(sys.argv)
if options.min_run:
    MIN_RUN = int(options.min_run)
if options.max_run:
    MAX_RUN = int(options.max_run)


if VERBOSE:
    print "checking directory = %s" % IDXA_DIR

# get list of directories, one run per directory
rundirs = [ d for d in listdir(IDXA_DIR) if os.path.isdir(join(IDXA_DIR,d)) ]

## production is currently run over one EVIO file at a time, but EventStore
## manages skims on a per-run basis, so we have to process the output from
## each file and merge them together for each run, then output the locations
## of where all the files should be stored
for rundir in sorted(rundirs):
    #if VERBOSE:
    print "checking run " + rundir
    # check that this is a directory with index information
    try:
        run = int(rundir)
    except ValueError:
        print "Invalid dir = %s" % (rundir)
        continue

    if run<MIN_RUN or run>MAX_RUN:
        continue

    ## test
    #if run != 2400:
    #    continue

    # merge the index files from the different files
    filedirs = [ d for d in listdir(join(IDXA_DIR,rundir)) ] 
    merged_skims = {}  # keep track of the different master skim files
    # setup output directory
    merged_skim_dir = join(IDXA_DIR,rundir,"EventStore")
    retval = system("mkdir -p -m %s %s"%(MKDIRMODE,merged_skim_dir))
    if retval != 0:
        print "Could not create EventStore directory, skipping run %d ...."%run
        system("echo \"%d\" >> failedruns.lst"%run)
        continue

    # file order matters to maintain the proper mapping between the order of events
    # in the skim and in the data files.  multi-threaded processing means that
    # we can't assume a strict ordering of events. 
    # so, always process files in numerical order
    for filedir in sorted(filedirs):
        filedir_fullpath = join(IDXA_DIR,rundir,filedir)
        if VERBOSE:
            print "checking file " + filedir

        # skip over any bad directories
        try: 
            filenum = int(filedir)
        except:
            continue

        # skip files that weren't skimmed (crashes, etc.)
        if not os.path.exists(join(filedir_fullpath,"idxafiles.txt")):
            continue

        # which skims are saved for this file?
        skims = parse_master_idxa_map(join(filedir_fullpath,"idxafiles.txt"))
        
        # merge skims on disk
        for (skim_name,skim_file) in skims.items():
            # if this is a new skim, open a new file
            if skim_name not in merged_skims.keys():
                merged_skims[skim_name] = open(join(merged_skim_dir,skim_name+"_skim.idxa"),"w")
                print>>merged_skims[skim_name], "IDXA"  # add header

            with open(skim_file) as f:
                # read the whole skim file into memory, since it shouldn't be too large
                lines = [line.strip() for line in f]
                if len(lines)<0:
                    continue
                # make sure the first line has the correct header
                if lines[0] != "IDXA":
                    continue
                linenum = 1
                while linenum < len(lines):
                    print>>merged_skims[skim_name], lines[linenum]
                    linenum += 1

    # write out skim mapping - save all the skims for REST files
    with open(join(merged_skim_dir,"rest_idxafiles.txt"),"w") as f:
        for (skim_name,skim_file) in sorted(merged_skims.items()):
            print>>f, "%s::%s" % (skim_name,join(merged_skim_dir,skim_name+"_skim.idxa"))
    # save a subset of skims for EVIO files
    if len(EVIO_SKIMS) > 0:
        with open(join(merged_skim_dir,"evio_idxafiles.txt"),"w") as f:
            for (skim_name,skim_file) in sorted(merged_skims.items()):
                if skim_name in EVIO_SKIMS:
                    print>>f, "%s::%s" % (skim_name,join(merged_skim_dir,skim_name+"_skim.idxa"))
                

    # close all files
    for (skim_name,skim_file) in merged_skims.items():
        skim_file.close()
                    
    # build injection inputs
    #eventstore_index_base_dir = join(BASEDIR,rundir)
    #system("mkdir -p " + eventstore_base_dir)
    #eventstore_files_dir = eventstore_base_dir + "/files"
    eventstore_evio_dbs_dir = join(BASEDIR,"evio_index",rundir)
    eventstore_rest_dbs_dir = join(BASEDIR,"rest_index",rundir)
    system("mkdir -p -m %s %s"%(MKDIRMODE,eventstore_files_dir + "/" + rundir))
    system("mkdir -p -m %s %s"%(MKDIRMODE,eventstore_evio_dbs_dir))
    system("mkdir -p -m %s %s"%(MKDIRMODE,eventstore_rest_dbs_dir))

    # using volatile disk
    #system("mkdir -p " + eventstore_files_dir + "/" + rundir)
    #system("mkdir -p " + eventstore_evio_dbs_dir + "/" + rundir)
    #system("mkdir -p " + eventstore_rest_dbs_dir + "/" + rundir)
 
    # location of EVIO files
    # NOTE: need to specialcase the "hd_raw_*.evio" file name
    with open(join(eventstore_files_dir,rundir,"evio_data_location.txt"),"w") as f:
        print>>f, "/mss/halld/%s/rawdata/Run%s/hd_rawdata_%s_*.evio" % (RUNPERIOD,rundir,rundir)
    # location of REST files
    with open(join(eventstore_files_dir,rundir,"rest_data_location.txt"),"w") as f:
        print>>f, "/work/halld/data_monitoring/%s/%s/REST/%s/dana_rest_%s_*.hddm" % (RUNPERIOD,DATAREVISION,rundir,rundir)
    # location where DB files for EVIO files will be generated
    with open(join(eventstore_files_dir,rundir,"evio_eventstore_location.txt"),"w") as f:
        print>>f, eventstore_evio_dbs_dir
    # location where DB files for REST files will be generated
    with open(join(eventstore_files_dir,rundir,"rest_eventstore_location.txt"),"w") as f:
        print>>f, eventstore_rest_dbs_dir
    # location of skim defitions for EVIO data
    with open(join(eventstore_files_dir,rundir,"evio_idxa_location.txt"),"w") as f:
        print>>f, join(IDXA_DIR,rundir,"EventStore","evio_idxafiles.txt")
    # location of skim defitions for REST data
    with open(join(eventstore_files_dir,rundir,"rest_idxa_location.txt"),"w") as f:
        print>>f, join(IDXA_DIR,rundir,"EventStore","rest_idxafiles.txt")


# this is needed for other steps
print "The base EventStore directory is =  %s" % BASEDIR

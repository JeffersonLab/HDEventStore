import os,sys
import subprocess

RESTDIR = "/w/halld-scifs1a/data_monitoring/RunPeriod-2014-10/ver11/REST"
SAVEDIR = "/w/halld-scifs1a/data_monitoring/RunPeriod-2014-10/ver11/REST/badfiles"


cmd= "find %s -name \*.hddm -exec ls -lh '{}' \\;"%RESTDIR
#cmd= "find %s -name \*.hddm -exec'"%RESTDIR

#p1 = subprocess.Popen(cmd.split() + ["ls -lh '{}'","\;"], stdout=subprocess.PIPE)
p1 = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
output = p1.communicate()[0]


for line in output.splitlines():
    tokens = line.split()
    print "%s %s"%(tokens[8],tokens[4])

#!/bin/csh
limit stacksize unlimited
limit descriptors 2048
set run=$1
echo building EventStore databases and indices for run $run ...
# set environment
######################## EVENTSTORE SETTINGS ###############################
setenv EVENTSTORE_OUTPUT_GRADE "recon-unchecked"
setenv EVENTSTORE_WRITE_TIMESTAMP "20150325"
setenv DATA_VERSION_NAME "recon_RunPeriod-2014-10_20150323_ver12"
#  
setenv EVENTSTORE_BASE_DIR "/work/halld/EventStore/RunPeriod-2014-10/ver12"
#############################################################################
source /home/gxproj3/setup_jlab_commissioning.csh  # need sim-recon for python HDDM libs
source /home/gxproj3/EventStore/setup.csh
source /home/gxproj3/EventStore/setup_python27_jlab.csh
# run the actual script
echo "Start Time = "`date`
./build_sqlite.sh $run
echo "End Time = "`date`

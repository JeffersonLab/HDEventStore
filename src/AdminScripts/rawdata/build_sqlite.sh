#!/bin/bash
#
# Author: Sean Dobbs (s-dobbs@northwestern.edu), 2014

trap fail 1 2 3 4 5 6 7 8 10 11 12 13 14 15
set
ulimit -n 256

######################## EVENTSTORE SETTINGS ###############################
export EVENTSTORE_OUTPUT_GRADE="mc-dc2-unchecked";
export EVENTSTORE_WRITE_TIMESTAMP="20140418";
export DATA_VERSION_NAME="mc-dc2_20140418_vs1";
#export OUT_DIR="";
#export INPUT_DIR="";
#############################################################################

echo $VERSION_ID
PROGNAME=`basename $0`

usage()
{
    echo "Usage: $PROGNAME"
    echo "you also have to set a raft of environment variables!!"
    exit 2
}


log()
{
    echo `/bin/date '+%Y/%m/%d %H:%M:%S'` $*
}


fail()
{
    echo
    echo "DONE:FAILED"
    exit 3
}


succeed()
{
    echo
    echo "DONE:SUCCEEDED"
    exit 0
}

checkEnvironment()
{
    # This function takes one argument: the name of a shell environment
    # variable. If it is defined and not the empty string, this function
    # returns. Otherwise it aborts.
    # There doesn't seem to be a nice way to double-dereference the shell
    # environment variable name in $1.
    value=`printenv $1`
    if [ -z "$value" ]
    then
	log "$PROGNAME: environment variable $1 was not set"
	fail
    fi
}


log $PROGNAME $*

export RUN_NUMBER=$1
export rundir=`echo $RUN_NUMBER | perl -n -e 'printf "%06d", $_;'`

export INPUT_DIR="/volatile/halld/RunPeriod-2014-10/offline_monitoring/ver09.1/EventStore/files/${rundir}"
export OUT_DIR="/volatile/halld/RunPeriod-2014-10/offline_monitoring/ver09.1/EventStore/evio_dbs/${rundir}"

if [ ! -e ${OUT_DIR} ]
then
    mkdir -p ${OUT_DIR}
fi

if [ ! -d ${OUT_DIR} ]
then
    log "Output path exists and is not a directory!"
    fail
fi


checkEnvironment "EVENTSTORE_OUTPUT_GRADE"
checkEnvironment "EVENTSTORE_WRITE_TIMESTAMP"
checkEnvironment "RUN_NUMBER"
checkEnvironment "OUT_DIR"
checkEnvironment "INPUT_DIR"
checkEnvironment "DATA_VERSION_NAME"

#WORKINGDIR=$OUT_DIR
#cd $WORKINGDIR

#export ESTOOLKIT=/home/sdobbs/EventStore/src/EventStoreToolkit
#. $ESTOOLKIT/setup.sh 

export mySVName=$DATA_VERSION_NAME

# setup output

#export EVENTSTORE_OUTPUT_DIR="${OUT_DIR}/index"
export EVENTSTORE_OUTPUT_DIR="${OUT_DIR}"

echo "OUTPUT PATH = $EVENTSTORE_OUTPUT_DIR"

#echo
#echo "subjob plan: $RUN_NUMBER-CO $OUT_DIR"
echo
echo "host= `/bin/hostname` shellpid= $$"

echo "Shell environment contains:"
echo
set
echo


echo "------------------------------------"
echo "  external script settings:"
echo "RUN_NUMBER=$RUN_NUMBER"
#echo "WORKINGDIR=$WORKINGDIR"
echo "------------------------------------"

log " Attempting to build the eventstore for the data file..."
    
#check to see if file with the location of the data files exists
if [ -e ${INPUT_DIR}/evio_data_location.txt ]
then
    # we store the names of the stub files in evio_data_location.txt 
    # but the actual EVIO rawdata files are staged from tape to /cache
    filesToInject=`cat ${INPUT_DIR}/evio_data_location.txt | awk '{print "/cache"$1}'`
    firstFileToInject=`echo $filesToInject | awk '{print $1}'`
    log found evio_data_location.txt
else
    log "Error -  data location file does not exist "
    log "Exiting before EventStore injection "
    fail
fi
    
# check to see if the file with the location of where the 
# eventstore is to write exist
if [ -e ${INPUT_DIR}/evio_eventstore_location.txt ]
then
    eventStoreOutputPath=`cat ${INPUT_DIR}/evio_eventstore_location.txt`
    log found evio_eventstore_location.txt
else
    log "Error - EventStore location file does not exist "
    log "Exiting before EventStore injection "
    fail
fi

#check to see if file with the location of the idxa files exists
if [ -e ${INPUT_DIR}/evio_idxa_location.txt ]
then
    idxaListPath=`cat ${INPUT_DIR}/evio_idxa_location.txt`
    log found evio_idxa_location.txt
    echo $idxaListPath
else
    log "Error -  IDXA output location file does not exist "
    log "Exiting before EventStore injection "
    fail
fi

eventStoreWriteTimeStamp=${EVENTSTORE_WRITE_TIMESTAMP}
eventStoreWriteGrade=${EVENTSTORE_OUTPUT_GRADE}
theDbFile=$eventStoreOutputPath/sqlite_${RUN_NUMBER}.db
theHistoryFile=$eventStoreOutputPath/logs/run${RUN_NUMBER}_sqlite.history
theLogFile=$eventStoreOutputPath/logs/run${RUN_NUMBER}_sqlite.log

echo
echo TimeStamp is $eventStoreWriteTimeStamp
echo Grade is $eventStoreWriteGrade
echo files are $filesToInject
echo the first file is $firstFileToInject
echo ES is $eventStoreOutputPath
echo DB is $theDbFile
echo History is $theHistoryFile
echo Log is $theLogFile
echo

rm -rf $eventStoreOutputPath
mkdir -p $eventStoreOutputPath
mkdir -p $eventStoreOutputPath/logs

# This is the actual write to the event store 
${ESTOOLKIT}/ESBuilder -add $filesToInject -dataVersionName ${mySVName} -db $theDbFile -output $eventStoreOutputPath  -grade $eventStoreWriteGrade  -historyfile $theHistoryFile -logFile $theLogFile -time $eventStoreWriteTimeStamp -dupRead $firstFileToInject -listOfParents NULL 

status_eventstore_data=$?

date
log "Done writing data files to eventstore "
log " Assembling idxa files for injection "

# parse idxa_location.txt to get the view and the file name                                                                                     
# format is : view::path_to_idxa_file/idxafile
 
status_view_inject=0
idxaList=`cat $idxaListPath`
for line in $idxaList
do
    theView=`echo $line | awk -F:: '{print $1}'`
    theIdxaFile=`echo $line | awk -F:: '{print $2}'`
    theIdxaLogFile=$eventStoreOutputPath"/logs/"$theView"_run"${RUN_NUMBER}".log"

    $eventStoreOutputPath/logs/run${RUN_NUMBER}_sqlite.log
    echo TimeStamp is $eventStoreWriteTimeStamp
    echo WriteGrade is  $eventStoreWriteGrade
    echo The view is $theView
    echo The DataVersionName is ${mySVName}
    echo The sqlite file is $theDbFile
    echo The EventStore Output is $eventStoreOutputPath
    echo The IDXA file is $theIdxaFile
    echo The Logfile is $theIdxaLogFile

# do the view injection                                                                                                                          
    ${ESTOOLKIT}/ESBuilder  -time $eventStoreWriteTimeStamp -grade $eventStoreWriteGrade -view $theView  -dataVersionName ${mySVName} -db $t\
heDbFile -output $eventStoreOutputPath -add $theIdxaFile -logFile $theIdxaLogFile -verbose
    mystatus=$?
    if [ $mystatus -eq 0 -a \
        $status_view_inject -eq 0 ]
    then
        status_view_inject=0
    else
        status_view_inject=1
    fi

done

log "View injection done with status " $status_view_inject
    
# are the pds and idxa injections both okay ?                                                                                                    
if [ $status_eventstore_data -eq 0 -a \
    $status_view_inject -eq 0 ]
then
    status_eventstore=0
else
    status_eventstore=1
fi
#                                                                                                                                                
date
log "EventStore job completed with status "  $status_eventstore 

if [  $status_eventstore -eq 0 ]
then
    succeed
else
    fail
fi

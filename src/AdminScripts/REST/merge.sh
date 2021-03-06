#!/bin/bash
# script to merge per-run DBs into the master DB

function contains() {
    local n=$#
    local value=${!n}
    for ((i=1;i < $#;i++)) {
        if [ "${!i}" == "${value}" ]; then
            echo "y"
            return 0
        fi
    }
    echo "n"
    return 1
}


## Configuration
export MyWorkDir=/work/halld/EventStore/RunPeriod-2014-10/ver11/merge
export MyESDir=/work/halld/EventStore/RunPeriod-2014-10/ver11/rest_index

export MasterDB=EventStore@hallddb.jlab.org:3306
#export MasterDB=/home/gxproj3/test/master.db 

# if we're passed an argument, assume it's a file containing a list of runs
if [ -n $1 ]; then
    if [ ! -f $1 ]; then
	echo Not a valid file: $1, exiting...
	exit 0
    fi
    # this requires bash 4 - I hope we can assume that we have this by this point\
    readarray -t runstoprocess < $1
fi

mkdir -p $MyWorkDir
cd $MyWorkDir
date
hostname
echo Master DB: $MasterDB
echo Creating list of all standalone databases in directory
echo $MyESDir
find $MyESDir -name "sqlite*.db" | awk '{print $1}' | sort > db_list.tmp
echo Found `cat db_list.tmp | wc -l` databases
echo

files=`cat db_list.tmp`
for file in $files
do
  #echo Merging $file
  filename=${file##*/}
  fname=${filename%.*}
  runnum=`echo $fname | awk 'BEGIN{FS="_"}{print $2}'`
  directory=${file%/*}
  #echo run number = "${runnum}"
  
  if [ -n "$runstoprocess" ]; then
      if [ $(contains "${runstoprocess[@]}" "${runnum}") == "n" ]; then
	  continue
      fi
  fi

  echo Merging $file

  cd $directory
  echo In directory `pwd`
  backupfile=backup_sqlite_${runnum}.tar
  if [ -e ${backupfile}.gz ]; then
      echo Backup file already exists - skip backup
  else
      echo Starting backup of standalone database for run $runnum
      tar cf ${backupfile} *.*
      gzip ${backupfile}
  fi

  mkdir -p mergeDB
  MyDataBase=$filename
  MyLogFile=${directory}/mergeDB/esdb.log
  MyHistFile=${directory}/mergeDB/esdb.history
  echo Start merging $MyDataBase

  $ESTOOLKIT/ESDB2DB -dbin ${MyDataBase} -dbout ${MasterDB} -logFile ${MyLogFile} -historyfile ${MyHistFile}

  if [ $? != 0 ]; then
     echo "Failed merging $file"
     if [ -e $MyWorkDir/failed_runs.lst ]
     then 
	 echo $file >> $MyWorkDir/failed_runs.lst
     else 
	 echo $file > $MyWorkDir/failed_runs.lst
     fi
  fi

  echo
  sleep 1
done
date

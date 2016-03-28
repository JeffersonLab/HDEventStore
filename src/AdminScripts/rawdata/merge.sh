#$ -S /bin/bash
#$ -j y

export MyWorkDir=/home/sdobbs/EventStore/src/EventStoreToolkit/hddmtest
export MyESDir=/home/sdobbs/EventStore/src/EventStoreToolkit/hddmtest

## THIS MUST BE SET CORRECTLY BEFORE RUNNING
##export MasterDB=EventStore@lnx151.lns.cornell.edu:3307
export MasterDB=/home/sdobbs/EventStore/src/EventStoreToolkit/hddmtest/master.db

export ESTOOLKIT=/home/sdobbs/EventStore/src/EventStoreToolkit
. $ESTOOLKIT/setup.sh 

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
  echo Merging $file
  filename=${file##*/}
  fname=${filename%.*}
  runnum=`echo $fname | awk 'BEGIN{FS="_"}{print $2}'`
  directory=${file%/*}

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

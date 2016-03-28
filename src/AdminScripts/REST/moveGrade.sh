#!/bin/bash
# script to change grades of data set

export MyDB=EventStore@hallddb.jlab.org:3306
#export MyDB=/testdir/master.db
export OldGrade=recon-unchecked
export NewGrade=recon
export MyDataVersionName=recon_RunPeriod-2014-10_20150301_ver11
export OldTime=20150304
export NewTime=20150301

export MyLogDir=/work/halld/EventStore/RunPeriod-2014-10/ver11/logs

export MyLogFile=${MyLogDir}/esdb_move.log
export MyHistFile=${MyLogDir}/esdb_move.history
mkdir -p $MyLogDir

date
echo Starting moveGrade

$ESTOOLKIT/ESVersionManager -db ${MyDB} -grade ${OldGrade} ${NewGrade} -time ${OldTime} ${NewTime} -dataVersionName ${MyDataVersionName} -logFile ${MyLogFile} -historyfile ${MyHistFile}

date

#!/bin/bash

export ESTOOLKIT=/home/sdobbs/EventStore/src/EventStoreToolkit
. $ESTOOLKIT/setup.sh

#export MyDB=EventStore@lnx151.lns.cornell.edu:3307
export MyDB=/home/sdobbs/EventStore/src/EventStoreToolkit/hddmtest/master.db
export OldGrade=mc-dc2-unchecked
export NewGrade=mc-dc2
export MyDataVersionName=pass2_20070330_FULL_1_20080122_data24_vs1
export OldTime=20140418
export NewTime=20140501

export MyLogDir=/home/sdobbs/EventStore/src/EventStoreToolkit/hddmtest

export MyLogFile=${MyLogDir}/esdb_move.log
export MyHistFile=${MyLogDir}/esdb_move.history
mkdir -p $MyLogDir

date
echo Starting moveGrade

$ESTOOLKIT/ESVersionManager -db ${MyDB} -grade ${OldGrade} ${NewGrade} -time ${OldTime} ${NewTime} -dataVersionName ${MyDataVersionName} -logFile ${MyLogFile} -historyfile ${MyHistFile}

date

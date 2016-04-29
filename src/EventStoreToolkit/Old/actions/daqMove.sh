#!/bin/bash

echo "You're $USER@$HOSTNAME"   ### CHECK
if [ "${HOSTNAME}" != "sol408.lns.cornell.edu" ]; then
   echo "daqMove.sh scripts should be run on sol408"
   exit 1
fi

if [ "${USER}" != "pass2" ]; then  ### CHECK
   echo "daqMove.sh scripts should be run under pass2 account"
   exit 1
fi

if [ -z $ESTOOLKIT ]; then
   echo "You need to define ESTOOLKIT environment pointing to your version of EventStoreToolkit"
   exit 1
fi

### CHECK
ESPort='3307'
ESSocket='/mnt/mysqlMaster/mysql.sock'
ESServer='lnx151.lns.cornell.edu'
ESOperator=vk@mail.lns.cornell.edu

# first make a backup of existing DB.
BACKUPDIR=/nfs/objy/EventStoreDB/ESDBDir/backup
BACKUP=EventStore.`date '+%Y-%m-%d'`.sql
if [ ! -f $BACKUPDIR/$BACKUP.bz2 ]; then
   mysqldump -u cleo --password='cleoc' -h $ESServer --port=$ESPort EventStore > $BACKUPDIR/$BACKUP
   bzip2 $BACKUPDIR/$BACKUP
fi

if [ -z $1 ]; then
     echo "Usage: daqMove.sh <dbName> <timeStamp=recentTimeStampInESDB>"
     echo "Example: daqMove.sh EventStoreTest"
     echo ""
     echo "         daqMove.sh EventStore 20090909"
     echo "         YOU ONLY NEED TO SUPLY TIMESTAMP when resolve conflicts"
     exit
fi
#if [ -z $2 ]; then
#     echo "Usage: daqMove.sh <dbName> <dataSetName>"
#     echo "Example: daqMove.sh EventStoreTest data37"
#     echo
#     exit
#fi

### Setup
POPDIR=/cdat/sol191/disk4/population/
curState=`ls -l $POPDIR`
EMAILLIST="vk@mail.lns.cornell.edu gregor@mail.lns.cornell.edu"
DBName=$1
Grades='daq-unchecked daq'
tomorrow=`python -c 'import time; print time.strftime("%Y%m%d",time.localtime(time.time()+24*60*60))'`
if [ -z $2 ]; then
#  newTimeStamp='0'
  newTimeStamp=`mysql -u cleo --password='cleoc' -h $ESServer --port=$ESPort -e "SELECT MAX(timeStamp) from Version where grade='daq'" -r -s EventStore  | tail -1`
else
  newTimeStamp=$2
fi
svName='daq'

# get maxRunNumber for daq grade
maxRunNumberInDaq=`mysql -u cleo --password='cleoc' -h $ESServer --port=$ESPort -e "SELECT MAX(maxRunNumber) from Version where grade='daq' and timeStamp=$newTimeStamp" -r -s EventStore  | tail -1`

# get graphid from ESDB
graphid=`mysql -u cleo --password='cleoc' -h $ESServer --port=$ESPort -e "SELECT DISTINCT graphid from Version where grade='daq' and timeStamp=$newTimeStamp" -r -s EventStore`

. $ESTOOLKIT/setup.sh

date
echo "Start transition of daq-unchecked to daq"

# define run list file which need to be excluded.
tmpDir=/tmp/$USER/EventStore
if [ ! -d $tmpDir ]; then
	mkdir -p $tmpDir
fi

runList=$tmpDir/good.runlist
if [ -f $runList ]; then
	/bin/rm -f $runList
fi

# get all runs from runlists
cat /home/pass1/cleo-c/Luminosities/runlists/data3[1-9].runlist /home/pass1/cleo-c/Luminosities/runlists/data[4-9][0-9].runlist | sort -n | awk '{if($4==1) print $1}' >> $runList

# TEMP
#runListFile=/home/pass1/cleo-c/Luminosities/runlists/$2.runlist
#if [ ! -f $runListFile ]; then
#   echo "$runListFile does not exist"
#   exit
#fi
#cat $runListFile | awk '{if($4==1) print $1}' >> $runList

# commented lines below are usefull for testing
#cp -f $DIR/sqlite.db $tmpDir
#cp -f $DIR/sqlite_daq.db $tmpDir/sqlite.db
#echo "Content of $tmpDir"
#ls -l $tmpDir

### PAUSE POPULATOR (STOP INJECTION TO EVENTSTORE)
if [ -f $POPDIR/PAUSE ]; then
   cat $POPDIR/PAUSE | Mail -s "Found $POPDIR/PAUSE file, please investigate" $EMAILLIST
   exit 1
fi

# from now on exit remove PAUSE file in POPDIR
trap '/cdat/sol191/disk4/MassStorage/populate/unpause_pop 2>&1 1>& $tmpDir/unpause_pop' 0
# create PAUSE file in POPDIR
/cdat/sol191/disk4/MassStorage/populate/pause_pop "daqMove" $tmpDir/pause_pop 2>&1
if [ $? -ne 0 ]; then
   cat $tmpDir/pause_pop | Mail -s "Pause populator failed from $HOSTNAME at `date`" $EMAILLIST
   exit 1
fi
echo "$POPDIR/PAUSE file created by `cat $POPDIR/PAUSE`"
sleep 30

# check existence of ACTIVE.$pid files in /cdat/sol191/disk4/population/

limit=20 # 20 minutes
counter=0
while [ -e $POPDIR/ACTIVE.* ]; do
   counter=$((counter+1))
   if [ $counter -gt $limit ]; then
      echo "'daq-unechecked' to 'daq' migration stale on pass2@$HOSTNAME with PID=$$. Please resolve this issue asap. $curState" | Mail -s "EventStore migration stale, there are active files in $POPDIR" $EMAILLIST
      exit 1
   fi
   sleep 60
   echo "We slept $counter-minutes"
done

### Invoke migration from 'daq-unechecked' to 'daq' grade
### use MySQL server
injectionLog=$tmpDir/ESVersionManager.log.`date +%Y-%m-%d`
echo "$ESTOOLKIT/ESVersionManager -grade $Grades -time 0 $newTimeStamp -goodRunList $runList -db ${DBName}@${ESServer}:${ESPort} -dataVersionName $svName"
$ESTOOLKIT/ESVersionManager -grade $Grades -time 0 $newTimeStamp -goodRunList $runList -db ${DBName}@${ESServer}:${ESPort} -dataVersionName $svName 2>&1 | tee $injectionLog

# usefull for debugging with SQLite
#$ESTOOLKIT/ESVersionManager -grade $Grades -time $Times -goodRunList $runList -dataVersionName $svName -db $tmpDir/sqlite.db
#/bin/cp -f $tmpDir/sqlite.db $DIR/sqlite_daq.db

# check status of ESVersionManager and if fail leave a message about backup.
if [ $? -ne 0 ]; then
   echo "FAIL"
   echo "Backup saved: $BACKUPDIR/$BACKUP.bz2"
   cat $injectionLog | mail -s "daqMove: fail to move daq grade" $ESOperator
   rm -f $injectionLog
else
   echo "SUCCESS"
   rm -f $BACKUPDIR/$BACKUP.bz2
fi

### RESUME POPULATOR (RESUME INJECTION TO EVENTSTORE)
/cdat/sol191/disk4/MassStorage/populate/unpause_pop 2>&1 1>& $tmpDir/unpause_pop
if [ $? -ne 0 ]; then
   cat $tmpDir/unpause_pop | Mail -s "Resume populator failed from $HOSTNAME at `date`" $EMAILLIST
   exit 1
else
   echo "Resume populator"
fi

if [ -d $tmpDir ]; then
	rm -rf $tmpDir
fi

# let's make truncation if we'll find truncated runs
echo "Look at /home/pass1/cleo-c/Luminosities/runlists/truncated/ for truncated runs"
TRUNCDIR=/home/pass1/cleo-c/Luminosities/runlists/truncated
for file in `ls $TRUNCDIR`; do
    runs=`cat $TRUNCDIR/$file | awk 'BEGIN {flag=0} {if(flag) print $2; if($1=="IDXD") flag=1;}' | sort -u`
    for run in $runs; do
        echo "Process run=$run"
        numViews=`mysql -u cleo --password='cleoc' -h $ESServer --port=$ESPort -e "SELECT view from KeyFile where run=$run and view like \"all%\" and graphid=$graphid" -r -s EventStore | sort -u | wc -l | sed "s/ *//"`
        if [ "$run" -lt "$maxRunNumberInDaq" ]; then
	   if [ "$numViews" -eq 1 ]; then
	      echo "Found run $run which is already injected into ESDB and cannot be truncated automatically" | mail -s "daqMove truncation failure" $ESOperator
	   fi
	   continue
	else
           echo "$ESTOOLKIT/ESFixTruncatedRun $TRUNCDIR/$file EventStore@$ESServer:$ESPort"
	   $ESTOOLKIT/ESFixTruncatedRun $TRUNCDIR/$file EventStore@${ESServer}:${ESPort}
	fi
    done
done

# finally we need to synchronize master and slave DB servers
#$ESTOOLKIT/ESSync

echo 'Job is completed'
date


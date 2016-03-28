#!/bin/bash

export PATH=/home/vk/Cornell/EventStore/util/dev/GroupEventStoreToolkit/:$PATH
if [ $# -ne 3 ]; then
   echo "Usage: removeRuns.sh <pw> <svName> <run>"
   exit
fi
echo "SpecificVersionName=$1, run=$2"
pw=$1
cmd="/usr/bin/mysql -N -B -u cleosoft --password='$pw' -h lnx151 --port=3307  EventStore -e"
ver=$2
run=$3
gid=`echo "$cmd 'select graphid from GraphPath, SpecificVersion where SpecificVersion.svid=GraphPath.svid and svName=\"$ver\"'" | /bin/sh`

echo "Found graphid=$gid"

echo
echo "Lookup Key files"
stm="select fileName from FileID, KeyFile where keyFileId=fileId and graphid=$gid and run=$run;"
keys=`echo "$cmd '$stm'" | /bin/sh`
for file in $keys;
do
   echo $file
done
echo
echo "Delete entries in ES"
for file in $keys;
do
   echo "delete from FileID where fileName=\"$file\";"
done

echo
echo "Entries in KeyFile table" 
stm="select * from KeyFile where graphid=$gid and run=$run;"
echo "$cmd '$stm'" | /bin/sh
echo
echo "Delete entries in ES"
echo "delete from KeyFile where graphid=$gid and run=$run;"

echo
echo "Entries in Location table"
stm="select * from Location where graphid=$gid and run=$run;"
echo "$cmd '$stm'" | /bin/sh
echo "Delete entries in ES"
echo "delete from Location where graphid=$gid and run=$run;"

echo
echo "Lookup location files"
stm="select fileName from FileID, Location where locationFileId=fileId and graphid=$gid and run=$run;"
lpds=`echo "$cmd '$stm'" | /bin/sh`
for file in $lpds;
do
   echo $file
#ESFileContent $file
done

echo 
echo "Delete entries in ES"
for file in $lpds;
do
   echo "delete from FileID where fileName=\"$file\";"
done


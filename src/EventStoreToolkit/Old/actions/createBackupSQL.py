#!/usr/bin/env python
"""A helper script which take a latest snapshot of EventStore DB and creates
SQL statements how to rollback to that state."""

import os, sys, string
import MySQLdb                          # the MySQL db data base

#
# main
#
if __name__ == "__main__":
    esdb="EventStore"
    if len(sys.argv)==2:
       if sys.argv[1]=="-help":
          print "Usage: createBackupSQL.py <esdb>"
          print "       takes latest snapshot of ESDB and create SQL statements how"
          print "       to come back to this db."
          sys.exit()
       esdb=sys.argv[1]

    #print "Connecting to MySQL database..."
    print "Reading from ",esdb,"..."
    db = MySQLdb.connect('lnx151.lns.cornell.edu','cleo','cleoc',esdb)
    cursor = db.cursor()

    query = "SELECT MAX(fileId) FROM FileID"
    cursor.execute(query)
    tup = cursor.fetchone()
    fileId = tup[0]

    query = "SELECT MAX(graphid) FROM Version"
    cursor.execute(query)
    tup = cursor.fetchone()
    graphid = tup[0]

    msg = """
    alter table FileID MODIFY fileId bigint UNSIGNED NOT NULL;
    delete from SpecificVersion where graphid>%s;
    delete from Version where graphid>%s;
    delete from FileID where fileId>%s;
    delete from KeyFile where keyFileId>%s;
    delete from Location where locationFileId>%s;
    alter table FileID MODIFY fileId bigint UNSIGNED AUTO_INCREMENT;
    """%(graphid,graphid,fileId,fileId,fileId)

    print msg


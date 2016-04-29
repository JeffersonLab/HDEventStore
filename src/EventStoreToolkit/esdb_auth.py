#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
#
"""This module provides L{md5crypt} authentication to EventStore MySQL DB"""

### NOTE: authentication needs to be revisited

import os, sys, string, stat, md5crypt, re

def dissassembleDBName(db):
    """Dissassemble the input db string.
    @type db: string
    @param db: input db string, e.g. EventStore@lnx151:3306:/var/log/mysql
    @rtype: tuple (dbName,dbHost,dbPort,dbSocket)
    @return: DB name, hostname of DB, its port and socket.
    """
    dbPort   = ""
    dbSocket = ""
    
    # check if db name doesn't have "%" character
    if string.find(db,"%")!=-1:
       raise "'%' is not allowed in DB name, if you'd like to specify port, please use ':' separator"

    # we need to parse db into dbName,dbHost,dbPort,dbSocket
    dbComponents = string.split(db,"@")
    if len(dbComponents)==2:
       dbName = dbComponents[0]
       content= string.split(dbComponents[1],":")
       dbHost = content[0]
       if len(content)>1 and content[1]:
          dbPort   = content[1]
       if len(content)==3 and content[2]:
          dbSocket = content[2]
    else:
       dbName = "EventStoreTMP"
       dbHost = dbComponents[0]
       dbPort = ""
       dbSocket = ""
    return (dbName,dbHost,dbPort,dbSocket)
	
def readConfigFile():
    """Read and parse content of $HOME/.esdb.conf configuration file
    The following syntax is supported in configuration file:
    # ESDB configuration file
    login:password
    # ESDB Master
    #ESMASTER=db@host:port:socket or /path/my/sqlite.db
    Comments are started with '#' letter. User can specify login and password
    to provide access to MySQL DB, if SQLite is used they're ignored. Also,
    user may specify which master DB to use by providing its host and DB names.
    """
    login = masterHost =  masterName = masterPort = masterSocket = admin = ""
    # read them from $HOME/.esdb.conf file
    uFileName = os.path.normpath(os.environ["HOME"]+"/.esdb.conf")
    if not os.path.isfile(uFileName):
       print "You should either create $HOME/.esdb.conf file with"
       print "user:password entry  or use -user/-password options"
       #sys.exit()
       return (login,admin,masterName,masterHost,masterPort,masterSocket)
    mode = os.stat(uFileName)[stat.ST_MODE]
    if mode!=33152:
       # mode is not -rw-------
       print "WARNING: permission of %s is not set to 0600 mode (-rw-------)"%uFileName
       os.chmod(uFileName,0600)
    with open(uFileName) as f:
        for read in f.readlines():
            line = string.split(read,"\n")[0]
            if not len(line): continue
            if line[0]=="#": continue
            # parse login information
            if re.search("LOGIN",line):
                login = string.split(line,"LOGIN=")[1]
            if re.search("ADMIN",line):
                admin = string.split(line,"ADMIN=")[1]
            if re.search("^[a-z]",line):
                login = line # for backward compatibility
            if re.search("ESMASTER",line):
                master= string.split(line,"ESMASTER=")[1]
                masterName,masterHost,masterPort,masterSocket=dissassembleDBName(master)
    return (login,admin,masterName,masterHost,masterPort,masterSocket)
    
def authToESMySQL(mysqlHost,userMySQL="",passwordMySQL="",adminLogin=""):
    """Provides authentication with EventStore DB."""
    #if not userMySQL or not passwordMySQL:
    # read them from $HOME/.esdb.conf file
    loginInfo,adminInfo,dbName,host,port,socket = readConfigFile()
    #print "In authToESMySQL()"
    #print "mysqlHost = %s userMySQL = %s passwordMySQL = %s adminLogin = %s"%(mysqlHost,userMySQL,passwordMySQL,adminLogin)
    #print "loginInfo = %s adminInfo = %s dbName = %s host = %s port = %s socket = %s"%(loginInfo,adminInfo,dbName,host,port,socket)
    try:
        if adminLogin:
            userMySQL,passwordMySQL = string.split(adminInfo,":")
        else:
            userMySQL,passwordMySQL = string.split(loginInfo,":")
    except:
        print "Fail to decode login/password information from your $HOME/.esdb.conf"
        raise
    # we used md5crypt.md5crypt(password,'') to generate these hashes
    #if mysqlHost=="localhost":
    md5user = ["$1$$e9gJaPbMtkdCzRfQ60OFF/","$1$$ix2ie71gC4Xwad5LhaC3S1"]
    md5pass = ["$1$$sbkdtWBHO51xPwl./H9N81","$1$$sbkdtWBHO51xPwl./H9N81"]
    if not md5user.count(md5crypt.unix_md5_crypt(userMySQL,"")):
       print "Fail to login to mysql server, incorrect login. Please try again."
       sys.exit()
    if not md5pass.count(md5crypt.unix_md5_crypt(passwordMySQL,"")):
       print "Fail to login to mysql server, incorrect password. Please try again."
       sys.exit()
    #return (userMySQL,passwordMySQL)
    # actual DB isn't protected by a password right now
    return (userMySQL,"")
#
# main
#
if __name__ == "__main__":
   print readConfigFile()
   print authToESMySQL('hallddb')  ### CHECK

#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004, 2005
# Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2005
#
"""Simple logger base class for EventStore"""

import string, logging

class ESLogger:
    """Simple logger base class for EventStore"""
    def __init__(self,logName,logType=(),formatType=""):
        """Set EventStore logger with logType tuple, e.g. ("stream",) or ("fileName",'a')
	It also support to choose formatType: short or long
	"""
	logging.addLevelName(60,"SQL")
        self.logger = logging.getLogger(logName)
	if string.lower(formatType)=="long":
	   self.logger.setLevel(logging.INFO)
	   self.formatter = logging.Formatter("%(process)d %(asctime)s %(name)s %(levelname)s - %(message)s")
	elif string.lower(formatType)=="debug":
	   self.logger.setLevel(logging.DEBUG)
#           self.formatter = logging.Formatter("%(process)d %(thread)d %(asctime)s %(name)s:%(module)s:%(lineno)d %(levelname)s - %(message)s")
	   self.formatter = logging.Formatter("%(process)d %(thread)d %(asctime)s %(name)s %(levelname)s - %(message)s")
	elif string.lower(formatType)=="short":
	   self.logger.setLevel(logging.INFO)
	   self.formatter = logging.Formatter("%(asctime)s %(levelname)s - %(message)s")
	else:
	   self.logger.setLevel(logging.INFO)
	   self.formatter = logging.Formatter("%(process)d %(asctime)s %(levelname)s - %(message)s")
	self.addHandler(logType)
	
    def setLevel(self,level=""):
        """Set logger level. Available levels are: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET"""
	if string.lower(level)=="critical":
	   self.logger.setLevel(logging.CRITICAL)
	elif string.lower(level)=="error":
	   self.logger.setLevel(logging.ERROR)
	elif string.lower(level)=="warning":
	   self.logger.setLevel(logging.WARNING)
	elif string.lower(level)=="info":
	   self.logger.setLevel(logging.INFO)
	elif string.lower(level)=="debug":
	   self.logger.setLevel(logging.DEBUG)
	else:
           self.logger.setLevel(logging.NOTSET)
	
    def addHandler(self,paramTuple):
        """Add new handler to the logger. Available handlers are: stream, file, socket, http"""
	if paramTuple:
	   try:
	      type  = paramTuple[0]
	      params= paramTuple[1:]
	   except:
	      raise
	else:
	   type  = ""
	   params= ""
	try:
	   if string.lower(type)=="stream":
	      channel= logging.StreamHandler(params)
	   elif string.lower(type)=="file":
	      channel= loggin.FileHandler(params)
	   elif string.lower(type)=="socket":
	      channel= loggin.SocketHandler(params)
	   elif string.lower(type)=="http":
	      channel= loggin.HTTPHandler(params)
	   else:	
	      channel= logging.StreamHandler()
	   channel.setFormatter(self.formatter)
	   self.logger.addHandler(channel)
	except:
	   raise

    def debug(self,msg):
        """Invoke logger debug printout"""
        self.logger.debug(msg)
	
    def info(self,msg):
        """Invoke logger info printout"""
        self.logger.info(msg)
	
    def warn(self,msg):
        """Invoke logger warn printout"""
        self.logger.warn(msg)
	
    def error(self,msg):
        """Invoke logger error printout"""
        self.logger.error(msg)
	
    def critical(self,msg):
        """Invoke logger critical printout"""
        self.logger.critical(msg)
	
    def logSQL(self,msg):
        """Invoke logger SQL printout"""
        self.logger.log(60,msg)


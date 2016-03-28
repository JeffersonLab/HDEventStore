#!/usr/bin/env python
#
# Author: Sean Dobbs (s-dobbs@northwestern.edu), 2014-03-05
#
# Original Version:
# Copyright 2008 Cornell University, Ithaca, NY 14853. All rights reserved.
# Author:  Valentin Kuznetsov, 2008
"""
EventSource MetaData web server
It depends on 
- cElementTree for parsing SOAP/XML, http://effbot.org/zone/element-index.htm
- cherrypy for web server, http://www.cherrypy.org
- SQLAlchemy for SQL access, http://www.sqlalchemy.org
"""

import string, os, sys, re, types
import xml.sax, xml.sax.handler
from   xml.sax.saxutils import escape

import cherrypy
from   cherrypy import expose
import sqlalchemy
from   sqlalchemy.sql import select

import cElementTree as et
import elementtree.ElementTree as ET

import logging, logging.handlers
from   optparse import OptionParser

import ConfigParser

def setCherryPyLogger(hdlr,logLevel):
    # set up logging for SQLAlchemy
    logging.getLogger('cherrypy.error').setLevel(logLevel)
    logging.getLogger('cherrypy.access').setLevel(logLevel)

    logging.getLogger('cherrypy.error').addHandler(hdlr)
    logging.getLogger('cherrypy.access').addHandler(hdlr)

def setSQLAlchemyLogger(hdlr,logLevel):
    # set up logging for SQLAlchemy
    logging.getLogger('sqlalchemy.engine').setLevel(logLevel)
    logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logLevel)
    logging.getLogger('sqlalchemy.pool').setLevel(logLevel)

    logging.getLogger('sqlalchemy.engine').addHandler(hdlr)
    logging.getLogger('sqlalchemy.orm.unitofwork').addHandler(hdlr)
    logging.getLogger('sqlalchemy.pool').addHandler(hdlr)


###############################################################################
### class DBManager
###############################################################################
class DBManager(object):
    def __init__(self, dbuser, dbpass, dbhost, dbname):
        self.engine = None
        # Initialize SQLAlchemy engines
        eName  = "mysql://%s:%s@%s/%s"%(dbuser, dbpass, dbhost, dbname)
        self.engine = sqlalchemy.create_engine(eName, strategy='threadlocal')
        self.tables = {}
        meta = sqlalchemy.MetaData()
        meta.bind = self.engine
        tables = []
        tlist = self.engine.table_names()
        for t in tlist: 
            self.tables[t]=sqlalchemy.Table(t, meta, autoload=True)

    def getruns(self, name, erange=None):
        query = """
SELECT DISTINCT r.run_number
FROM Raw_Run_Properties r, Dataset d, Energy_Class e
WHERE r.run_number is not null """
        if  name:
            query += """
AND d.dataset_name = :dname
AND r.run_number >= d.first_run_number
AND r.run_number <= d.last_run_number """
        if  erange:
            query += """
AND e.energy_range_name = :erange
AND r.beam_energy >= e.min_beam_energy
AND r.beam_energy <= e.max_beam_energy """
        query += "ORDER BY r.run_number ASC"
        sel    = sqlalchemy.text(query)
        con    = self.engine.connect()
        result = con.execute(sel, dname=name, erange=erange)
        runs   = (int(run[0]) for run in result)
        con.close()
        return runs

    def getenergynames(self):
        query  = """SELECT d.energy_range_name FROM Energy_Class d 
WHERE d.energy_range_name is not null ORDER BY d.energy_range_name ASC"""
        sel    = sqlalchemy.text(query)
        con    = self.engine.connect()
        result = con.execute(sel)
        names  = (name[0] for name in result)
        con.close()
        return names

    def getdatasetnames(self):
        query  = """SELECT d.dataset_name FROM Dataset d 
WHERE d.dataset_name is not null ORDER BY d.dataset_name ASC"""
        sel    = sqlalchemy.text(query)
        con    = self.engine.connect()
        result = con.execute(sel)
        names  = (name[0] for name in result)
        con.close()
        return names

    def getrunranges(self, name, erange):
        query  = """SELECT first_run_number, last_run_number 
FROM Dataset WHERE dataset_name = :dname"""
        sel    = sqlalchemy.text(query)
        con    = self.engine.connect()
        result = con.execute(sel, dname=name)
        runs   = ((int(run[0]), int(run[1])) for run in result)
        con.close()
        return runs

    def getboundaries(self, ename):
        query = """SELECT energy_range_name, min_beam_energy, max_beam_energy 
FROM Energy_Class WHERE energy_range_name = :ename"""
        sel    = sqlalchemy.text(query)
        con    = self.engine.connect()
        result = con.execute(sel, ename=ename)
        bounds = ((item[0], float(item[1]), float(item[2])) for item in result)
        con.close()
        return bounds

    def getproperties(self, srun, erun):
        query  = """SELECT run_number, start_time, event_count, 
last_event_number, luminosity, beam_energy, magnetic_field, 
run_type, runmanager_approved 
FROM Raw_Run_Properties WHERE run_number >= :r1 and run_number <= :r2"""
        sel    = sqlalchemy.text(query)
        con    = self.engine.connect()
        result = con.execute(sel, r1=srun, r2=erun)
        runs   = ((int(i[0]), i[1], int(i[2]), int(i[3]), \
                   float(i[4]), float(i[5]), float(i[6]), i[7], i[8] ) \
                  for i in result)
        con.close()
        return runs

###############################################################################
### class ESMetaDataServer
###############################################################################
class ESMetaDataServer(object):
    def __init__(self,dbmgr, logName='/tmp/esmetadb-cmds.log',verbose=0):
        self.dbmgr = dbmgr
        self.verbose=verbose
        hdlr = logging.handlers.TimedRotatingFileHandler( logName, 'midnight', 1, 7 )
        formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
        hdlr.setFormatter( formatter )
        self.logger = logging.getLogger("CMDServer")
        if verbose:
           logLevel=logging.DEBUG
        else:
           logLevel=logging.INFO
        self.logger.setLevel(logLevel)
        self.logger.addHandler(hdlr)
        setSQLAlchemyLogger(hdlr,logLevel)
        setCherryPyLogger(hdlr,logLevel)

    def writeLog(self,_msg):
        if type(_msg) is not types.StringType:
           msg=repr(_msg)
        else:
           msg=_msg
        if self.verbose:
           self.logger.debug(msg)
        else:
           self.logger.info(msg)
           
    @expose
    def index(self,**kwargs):
        self.namespace_expr = re.compile(r'^\{.*\}')
        # get request data and produce an ElementTree that we can work with.
        request = cherrypy.request
	if  self.verbose > 1:
	    self.writeLog(request)
        
        response = cherrypy.response
        if  not request.headers.has_key('Content-Length'):
            page  = "<h4>EventSource Meta Data Service</h4>"
            page +="""<pre>
The following operations are supported.

    * GetEnergyBoundaries(Energy_Range_Name)
      Get energy boundaries for a given energy name.

    * GetEnergyRangeNames()
      Query for a list of valid energy range names.

    * GetRuns(Dataset_Name, Energy_Range_Name)
      Query by dataset and energy range to return a list of runs. Both parameters are optional.

    * GetRawRunProperties(Start_Run_Number, End_Run_Number)
      Query and return an array of Raw_Run_Properties.

    * GetDatasetNames()
      Get a list of valid dataset names.

    * GetRunRanges(Dataset_Name, Energy_Range_Name)
      Query by dataset and energy range to return a list of run ranges. Both parameters are optional. 
</pre>
"""
            return page

        clen = int(request.headers.get('Content-Length')) or 0
        data = request.body.read(clen)
	if  self.verbose > 1:
	    self.writeLog(data)
        
        request.soap_start = data[:2048]
        soapreq = et.fromstring(data)
        #self.writeLog(soapreq)

        # find the body of the request and the specific method name that has
        # been requested.
        body = soapreq.find("{http://schemas.xmlsoap.org/soap/envelope/}Body")
        #self.writeLog("body0="+repr(body))
        body = body.getchildren()[0]
        #self.writeLog("body1="+repr(body))

        methodname = body.tag
        methodname = self.namespace_expr.sub("", methodname)
        #self.writeLog(methodname)

        request.soap_method = methodname

        method=getattr(self,methodname)
	if  self.verbose > 1:
	    self.writeLog(method)

        params = {"_ws" : True}
        params["xml_body"] = body
        return method(**params)

    def setContentType(self,type):
        """
           Set CherryPy Content-Type to provided type
           @type type: string
           @param type: type of application, "text/xml" or "text/html"
           @rtype: none
           @return: none
        """
        if  int(string.split(cherrypy.__version__,".")[0])==3:
            if type=="xml":
               cherrypy.response.headers['Content-Type']='text/xml;charset=utf-8'
            else:
               cherrypy.response.headers['Content-Type']='text/html'
        elif int(string.split(cherrypy.__version__,".")[0])==2:
            if type=="xml":
               cherrypy.response.headerMap['Content-Type'] = "text/xml;charset=utf-8"
            else:
               cherrypy.response.headerMap['Content-Type'] = "text/html"

    def soapTopEnvelop(self):
        page="""
<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
"""
        return page

    def soapBottomEnvelop(self):
        page="""
  </soap:Body>
</soap:Envelope>
"""
        return page

    def decode(self, xml_body):
	if  self.verbose > 1:
            self.writeLog("%s %s"%(type(xml_body),repr(xml_body)))
        xmlItems=xml_body.getchildren()
        inputdict = {}
        for item in xmlItems:
            inputdict[self.namespace_expr.sub("", item.tag)] = item.text
        return inputdict

    @expose
    def GetRuns(self, **kwargs):
        # I need to parse incoming XML message
        inputdict = self.decode(kwargs['xml_body'])
        #print "###GetRuns", inputdict
        name  = inputdict['Dataset_Name']
        erange = None
        if  inputdict.has_key('Energy_Range_Name'):
            erange= inputdict['Energy_Range_Name']
        runs  = dbmgr.getruns(name, erange)
        srun  = ""
        for run in runs:
            srun += "\n        <int>%s</int>" % run
        self.setContentType('xml')
        page =self.soapTopEnvelop()
        page+="""
    <GetRunsResponse xmlns="http://gluex.org/GlueX">
      <GetRunsResult>
%s
      </GetRunsResult>
    </GetRunsResponse>
        """ % srun
        page+=self.soapBottomEnvelop()
	if  self.verbose > 1:
            self.writeLog(page)
        return page

    @expose
    def GetEnergyRangeNames(self,**kwargs):
        inputdict = self.decode(kwargs['xml_body'])
        #print "###GetEnergyRangeNames", inputdict
        enames = dbmgr.getenergynames() 
        snames = ""
        for name in enames:
            snames += "\n        <string>%s</string>" % name
        self.setContentType('xml')
        page =self.soapTopEnvelop()
        page+="""
    <GetEnergyRangeNamesResponse xmlns="http://gluex.org/GlueX">
      <GetEnergyRangeNamesResult>
%s
      </GetEnergyRangeNamesResult>
    </GetEnergyRangeNamesResponse>
""" % snames
        page+=self.soapBottomEnvelop()
	if  self.verbose > 1:
            self.writeLog(page)
        return page

    @expose
    def GetDatasetNames(self,**kwargs):
        inputdict = self.decode(kwargs['xml_body'])
        #print "###GetDatasetNames", inputdict
        dnames = dbmgr.getdatasetnames() 
        snames = ""
        for name in dnames:
            snames += "\n        <string>%s</string>" % name
        self.setContentType('xml')
        page =self.soapTopEnvelop()
        page+="""
    <GetDatasetNamesResponse xmlns="http://gluex.org/GlueX">
      <GetDatasetNamesResult>
%s
      </GetDatasetNamesResult>
    </GetDatasetNamesResponse>
        """ % snames
        page+=self.soapBottomEnvelop()
	if  self.verbose > 1:
            self.writeLog(page)
        return page

    @expose
    def GetRunRanges(self,**kwargs):
        inputdict = self.decode(kwargs['xml_body'])
        ename  = inputdict['Dataset_Name']
        erange = inputdict['Energy_Range_Name']
        ranges = dbmgr.getrunranges(ename, erange) 
        #print "###GetRunRanges", inputdict
        snames = ""
        for srun, erun in ranges:
            snames += "\n          <RunRange>"
            snames += "\n              <StartRunNumber>%s</StartRunNumber>" % srun
            snames += "\n              <EndRunNumber>%s</EndRunNumber>" % erun
            snames += "\n          </RunRange>"

        self.setContentType('xml')
        page =self.soapTopEnvelop()
        page+="""
    <GetRunRangesResponse xmlns="http://gluex.org/GlueX">
      <GetRunRangesResult>
%s
      </GetRunRangesResult>
    </GetRunRangesResponse>
""" % snames
        page+=self.soapBottomEnvelop()
	if  self.verbose > 1:
            self.writeLog(page)
        return page

    @expose
    def GetEnergyBoundaries(self,**kwargs):
        inputdict = self.decode(kwargs['xml_body'])
        #print "###GetEnergyBoundaries", inputdict
        erange = inputdict['Energy_Range_Name']
        bounds = dbmgr.getboundaries(erange) 
        snames = ""
        for name, min_e, max_e in bounds:
            snames += "\n          <EnergyClass>"
            snames += "\n              <EnergyRangeName>%s</EnergyRangeName>" % name
            snames += "\n              <MinBeamEnergy>%s</MinBeamEnergy>" % min_e
            snames += "\n              <MaxBeamEnergy>%s</MaxBeamEnergy>" % max_e
            snames += "\n          </EnergyClass>"
        self.setContentType('xml')
        page =self.soapTopEnvelop()
        page+="""
    <GetEnergyBoundariesResponse xmlns="http://gluex.org/GlueX">
      <GetEnergyBoundariesResult>
%s
      </GetEnergyBoundariesResult>
    </GetEnergyBoundariesResponse>
""" % snames
        page+=self.soapBottomEnvelop()
	if  self.verbose > 1:
            self.writeLog(page)
        return page

    @expose
    def GetRawRunProperties(self,**kwargs):
        inputdict = self.decode(kwargs['xml_body'])
        srun = inputdict['Start_Run_Number']
        erun = inputdict['End_Run_Number']
        properties = dbmgr.getproperties(srun, erun) 
        #print "###GetEnergyBoundaries", inputdict
        snames = ""
        for run, stime, evtcount, last_evt, lumi, beam_e, mag_field, run_type, runmgr in properties:
            snames += "\n         <Raw_Run_Properties>"
            snames += "\n            <Run_Number>%s</Run_Number>" % run
            snames += "\n            <Start_Time>%s</Start_Time>" % stime
            snames += "\n            <Event_Count>%s</Event_Count>" % evtcount
            snames += "\n            <Last_Event_Number>%s</Last_Event_Number>" % last_evt
            snames += "\n            <Luminosity>%s</Luminosity>" % lumi
            snames += "\n            <Beam_Energy>%s</Beam_Energy>" % beam_e
            snames += "\n            <Magnetic_Field>%s</Magnetic_Field>" % mag_field
            snames += "\n            <Run_Type>%s</Run_Type>" % run_type
            snames += "\n            <Runmanager_Approved>%s</Runmanager_Approved>" % runmgr
            snames += "\n         </Raw_Run_Properties>"
        self.setContentType('xml')
        page =self.soapTopEnvelop()
        page+="""
    <GetRawRunPropertiesResponse xmlns="http://gluex.org/GlueX">
      <GetRawRunPropertiesResult>
%s
      </GetRawRunPropertiesResult>
    </GetRawRunPropertiesResponse>
""" % snames
        page+=self.soapBottomEnvelop()
	if  self.verbose > 1:
            self.writeLog(page)
        return page
    GetRawRunProperties.exposed=True

###############################################################################
### class CMDOptionParser
###############################################################################
class CMDOptionParser:
    def __init__(self):
        self.parser = OptionParser()

        self.parser.add_option("-v","--verbose",action="store", type="int", default=0, 
             dest="verbose", help="specify verbosity level, 0-no, 1-little, 2-extreme")
        self.parser.add_option("--log",action="store", type="string", 
             default='/tmp/esmetadb-cmds.log', dest="log",help="specify location of the server logger")
        self.parser.add_option("-s","--server",action="store", type="string", 
             default='localhost', dest="server",help="address to bind to")
        self.parser.add_option("-p","--port",action="store", type="int", 
             default=62000, dest="port",help="port to run server on")

    def getOpt(self):
        """
            Returns parse list of options
        """
        return self.parser.parse_args()

###############################################################################
### class Handler
###############################################################################
class Handler (xml.sax.handler.ContentHandler):
    def startElement(self, name, attrs):
        if name=='Dataset_Name':
           print "Handler",name,attrs


###############################################################################
### main function
###############################################################################
if __name__ == '__main__':
   
   cp = ConfigParser.ConfigParser()
   cp.readfp(open('DBParam'))
   dbuser = cp.get('Global','user')
   dbpass = cp.get('Global','pass')
   dbhost = cp.get('Global','host')
   dbname = cp.get('Global','db')
   dbmgr  = DBManager(dbuser, dbpass, dbhost, dbname)

   optManager  = CMDOptionParser()
   (opts,args) = optManager.getOpt()
   manager=ESMetaDataServer(dbmgr, logName=opts.log,verbose=opts.verbose)
   global_conf = {'global': {'engine.autoreload.on': False,
		             'server.socket_port': opts.port,
                             'server.thread_pool': 20, 
                             'server.socket_host': opts.server, 
                             'tools.log_tracebacks.on':True,
#                             'tools.proxy.on':True,
#                             'tools.proxy.base': 'http://server/proxy/cmds/',
#                             'tools.proxy.base': 'http://server',
#                             'tools.proxy.local':'',
			     'environment':'production'}
		 }
   # update cherrypy global configuration
   cherrypy.config.update(global_conf)

   conf = {'/' : {'tools.staticdir.root': os.getcwd()} }
   cherrypy.quickstart(manager, '/', config = conf)

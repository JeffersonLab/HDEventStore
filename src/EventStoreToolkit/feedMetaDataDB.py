#!/usr/bin/env python
#
# Ported to GlueX by Sean Dobbs (s-dobbs@northwestern.edu), 2014
#
# Copyright 2004 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2004
# Version: $Id: feedMetaDataDB.py,v 1.9 2007/01/23 19:16:33 gregor Exp $
"""
MetaData injector tool. It is based on httplib to form a SOAP message
For full description of WSDL and related stuff please visit
http://cougar.cs.cornell.edu/CLEO/CLEO_admin.asmx
"""

import os, sys, string, re, httplib, urllib, inspect
import smtplib, traceback

HOST= "esdb.research.northwestern.edu"
PORT= 80
NS  = "http://cleo.lepp.cornell.edu/CLEO/"  ### FIX
WSDL= "http://cougar.cs.cornell.edu/CLEO/test_CLEO_admin.asmx?WSDL"   ### FIX
# For sending email messages about problems
FROMADDR= "eventstore@gluex.org"
TOADDRS= [ "s-dobbs@northwestern.edu" ]

def sendEmail( msgText ):
    message = "From: " + FROMADDR + \
	      "\r\nTo: " + ", ".join( TOADDRS ) + \
	      "\r\nSubject: Error injecting metadata\r\n\r\n" + msgText
    server = smtplib.SMTP( 'localhost' )
    server.sendmail( FROMADDR, TOADDRS, message )
    server.quit

def parseWSDL(wsdl=WSDL):
    """Parse a wsdl file. So far we use urllib to do a job to read content of the file"""
    data = urllib.urlopen(wsdl).read()
    print data
    return data
    
def soapEnvelope():
    """CHANGE THIS: Form a soap envelop in a form of CS web service wants. For service description,
    please consult http://cougar.cs.cornell.edu/CLEO/CLEO_WS.asmx"""
    
    envelope="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">"""
    return envelope
    
def headerEnvelope(userName="CLEOadmin",password="CLEOpassword"):
    """Form a header of soap envelop which include user authentication"""
    envelope="""
<soap:Header>
    <AuthHeader xmlns="http://gluex.org/GlueX">
      <Username>%s</Username>
      <Password>%s</Password>
    </AuthHeader>
</soap:Header>"""%(userName,password)
    return envelope
    
def soapBody(method,argList):
    """Form a body of soap envelop. Construct appropriate array of items to retrieve."""
    envelope="""
  <soap:Body>
    <%s xmlns="http://cleo.lepp.cornell.edu/CLEO">"""%method
    for item in argList:
        argName=item[0]
	argValue=item[1]
	if argName=="file":
	   aList = ["Run_Number","Start_Time","Event_Count","Last_Event_Number","Luminosity","Beam_Energy","Magnetic_Field","Run_Type","Runmanager_Approved"]
	   lines = open(argValue).readlines()
	   envelope+="\n      <ArrayOfRawRunProperties>"
	   for idx in xrange(0,len(lines)):
	       s = string.split(lines[idx],",")
	       if len(s)!=len(aList):
	          print "ERROR: supplied list of values does not correspond to list of parameters"
		  print "parameters=",aList
		  print "values=",s
	       envelope+="\n      <Raw_Run_Properties>"
	       for i in xrange(0,len(s)):
		   value=s[i]
		   if i==len(s)-1: # to remove \n
		      value=string.split(s[i])[0]
	           envelope+="\n        <"+aList[i]+">"+value+"</"+aList[i]+">"
	       envelope+="\n      </Raw_Run_Properties>"
	   envelope+="\n      </ArrayOfRawRunProperties>"
	else:
           envelope+="\n      <%s>%s</%s>"%(argName,argValue,argName)
    envelope+="""
    </%s>
  </soap:Body>"""%method
    return envelope
    
def endEnvelope():
    """Add end statement to soap envelop"""
    envelope="""</soap:Envelope>"""
    return envelope

def constructSOAPEnvelope(method,aList):
    """Construct a soap envelop for given method and argument list"""
    envelope=soapEnvelope()+headerEnvelope()+soapBody(method,aList)+endEnvelope()
    return envelope
    
def sendSOAPMessage(method,envelope,test=1,debug=0):
    """Send soap message to cougar.cs.cornell.edu. Right now we use httplib to do a job"""
    try:
	if debug:
	    httplib.HTTPConnection.debuglevel = 1
	http_conn = httplib.HTTP(HOST,PORT)
	if test:
	    print "WARNING: using test database"
	    http_conn.putrequest('POST','/CLEO/test_CLEO_admin.asmx')
	else:
	    http_conn.putrequest('POST','/CLEO/CLEO_admin.asmx')
        http_conn.putheader('Host',HOST)
        http_conn.putheader('Content-Type','text/xml; charset=utf-8')
        http_conn.putheader('Content-Length',str(len(envelope)))
        http_conn.putheader('SOAPAction',NS+method)
        http_conn.endheaders()
        http_conn.send(envelope)

	(status_code,msg,reply)=http_conn.getreply()
	response=http_conn.getfile().read()
	if debug or msg!="OK":
	    print
	    print http_conn.headers
	    print "*** Outgoing SOAP ", "*"*54
	    print envelope
	    print "*"*72
	    print "status code:",status_code
	    print "message:",msg
	    print "*"*72
	    print reply
	    print "*** Incoming SOAP ", "*"*54
	    print response
    except:
	# Write out the exception to stderr
        sys.excepthook( sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2] )
	# And email it...
	errmsg = "Got an exception sending metadata to " + NS + \
	      "\r\n" + traceback.format_exc()
	sendEmail( errmsg )


if __name__ == "__main__":
#   parseWSDL(WSDL)
   x = 1
   verbose = 0
   test    = 1
   usage ="""feedMetaDataDB.py [ -help ] [ -listServices ] [ -verbose ]
           [ [ -inject ] -<serviceName> [<param>=<value> <param>=<value>] ]
	   
           Examples: feedMetaDataDB.py -DeleteEnergyClass Energy_Range_Name=data99
	             feedMetaDataDB.py -AddRawRunPropertiesArray <fileName>
   """
   if len(sys.argv)==1:
      print usage
      sys.exit() 
   index=0
   while x < len(sys.argv):
     index+=1
     if index==20: break
     print x,sys.argv[x]
     if sys.argv[x]=="-verbose":
        verbose=1
	x+=1
     if sys.argv[x]=="-listServices":
        print "CLEO_admin services:"
	print "- AddEnergyClass(Energy_Range_Name,Min_Beam_Energy,Max_Beam_Energy)"
	print "  Add a new energy range\n"
	print "- AddDataset(Dataset_Name,First_Run_Number,Last_Run_Number)"
	print "  Add a new dataset\n"
	print "- AddRawRunProperties(Run_Number,Start_Time,Event_Count,Last_Event_Number,Luminosity,Beam_Energy,Magnetic_Field,Run_Type,Runmanager_Approved)"
	print "  Add Raw_Run_Properties for one run\n"
	print "- AddRawRunPropertiesArray <fileName>"
	print "  Add a run to Raw_Run_Properties\n"
	print "- DeleteEnergyClass(Energy_Range_Name)"
	print "  Delete an energy range name\n"
	print "- DeleteDataset(Dataset_Name)"
	print "  Delete a dataset\n"
	print "- DeleteRawRunProperties(Run_Number)"
	print "  Delete a row from Raw_Run_Properties\n"
	print "- UpdateRawRunProperties(Run_Number,Start_Time,Event_Count,Last_Event_Number,Luminosity,Beam_Energy,Magnetic_Field,Run_Type,Runmanager_Approved)"
	print "  Update Raw_Run_Properties of specified run number\n"
	sys.exit()
     if sys.argv[x]=="-help":
        print usage
	sys.exit()
     if sys.argv[x]=="-inject":
        test = 0
	x+=1
     if x < len(sys.argv) and sys.argv[x][0]=="-" and sys.argv[x]!="-inject":
	service=sys.argv[x][1:]
	x+=1
	listArgs=sys.argv[x:]
	args = []
	for item in listArgs:
	    if item[0]=="-": break
	    # special case when fileName is supplied
	    if os.path.isfile(item):
	       args.append(["file",item])
	       x+=1
	       break
	    name=""
	    aList=[]
	    if string.find(item,"=")==-1:
	       print "You need to supply argument name for",item
	       sys.exit()
	    else:
	       ss=string.split(item,"=")
	       name = ss[0]
	       value= ss[1]
	    aList.append(name)

	    if re.match("\A\d+\.\d+\Z", value):
		aList.append(float(value))
	    else:
		if re.match("\A\d+\Z", value):
		    aList.append(int(value))
		else: # treat it as a string
		    aList.append(value)
	    args.append(aList)
	    x+=1
	print "Processing %s("%service,
	secondParam=0
	for param in args:
	    print "'%s=%s' "%(param[0],param[1]),
	print ")"
   envelope=constructSOAPEnvelope(service,args)
   sendSOAPMessage(service,envelope,test,verbose)


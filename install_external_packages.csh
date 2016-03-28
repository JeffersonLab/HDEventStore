#!/bin/tcsh

# this script assumes you have already set the environment
source setup_python27_jlab.csh

if ( ! -d $EVENTSTORE_LIBDIR ) then
	mkdir -p $EVENTSTORE_LIBDIR
endif
if ( ! -d $EVENTSTORE_LIB64DIR ) then
	mkdir -p $EVENTSTORE_LIB64DIR
endif
if ( ! -d $EVENTSTORE_PYTHONDIR ) then
	mkdir -p $EVENTSTORE_PYTHONDIR
endif

set PREFIX=$EVENTSTORE_BASE/$BMS_OSNAME
# pull the source packages from the web
set WEBDIR=https://halldweb1.jlab.org/dist/EventStore

# tarballs for main EventStore packages
easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/docutils-0.12.tar.gz
easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/epydoc-3.0.1.zip
easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/fpconst-0.7.2.tar.gz
easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/MySQL-python-1.2.4b4.tar.gz
easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/wstools-0.4.3.tar.gz
easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/SOAPpy.zip

# Metadata DB
easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/CherryPy-3.2.4.tar.gz
easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/SQLAlchemy-0.9.8.tar.gz 
easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/elementtree-1.2.6-20050316.tar.gz
easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/cElementTree-1.0.5-20051216.tar.gz
#easy_install --install-dir=$EVENTSTORE_PYTHONDIR $WEBDIR/




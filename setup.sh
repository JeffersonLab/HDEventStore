#!/bin/bash

if ( ! $?BMS_OSNAME ) then
    echo "BMS_OSNAME not set!"
endif

export EVENTSTORE_BASE=/home/gxproj3/EventStore
export EVENTSTORE_LIBDIR=${EVENTSTORE_BASE}/${BMS_OSNAME}/lib
export EVENTSTORE_LIB64DIR=${EVENTSTORE_BASE}/${BMS_OSNAME}/lib64
# revist this - maybe set it up like a standard site-packages setup?
export EVENTSTORE_PYTHONDIR=${EVENTSTORE_LIBDIR}/python

export PATH=${EVENTSTORE_BASE}/${BMS_OSNAME}/bin:${PATH}
export LD_LIBRARY_PATH=${EVENTSTORE_LIBDIR}:${EVENTSTORE_LIB64DIR}:${LD_LIBRARY_PATH}
export PYTHONPATH=${EVENTSTORE_PYTHONDIR}:${PYTHONPATH}

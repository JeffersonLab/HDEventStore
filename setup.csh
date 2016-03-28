#!/bin/tcsh

if ( ! $?BMS_OSNAME ) then
    echo "BMS_OSNAME not set!"
endif

setenv EVENTSTORE_BASE /home/gxproj3/EventStore
setenv EVENTSTORE_LIBDIR ${EVENTSTORE_BASE}/${BMS_OSNAME}/lib
setenv EVENTSTORE_LIB64DIR ${EVENTSTORE_BASE}/${BMS_OSNAME}/lib64
# revist this - maybe set it up like a standard site-packages setup?
setenv EVENTSTORE_PYTHONDIR ${EVENTSTORE_LIBDIR}/python

setenv ESTOOLKIT ${EVENTSTORE_BASE}/src/EventStoreToolkit

setenv PATH ${EVENTSTORE_BASE}/${BMS_OSNAME}/bin:${PATH}
setenv LD_LIBRARY_PATH ${EVENTSTORE_LIBDIR}:${EVENTSTORE_LIB64DIR}:${LD_LIBRARY_PATH}
setenv PYTHONPATH ${EVENTSTORE_PYTHONDIR}:${PYTHONPATH}

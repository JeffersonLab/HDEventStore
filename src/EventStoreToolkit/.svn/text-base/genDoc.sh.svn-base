#!/bin/sh

# setup environment
. `dirname $0`/setup.sh

epydoc --check -o Doc/api -n "EventStoreToolkit API" *.py
rm -rf pdf
epydoc -o pdf --pdf -n "EventStoreToolkit API" *.py
epydoc -o Doc/api -n "EventStoreToolkit API" *.py
mv -f pdf/api.pdf Doc/api/
rm -rf pdf

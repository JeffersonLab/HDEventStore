#!/usr/bin/env python

import sys
lines = open('raw_run_properties.sql')
ofile = open('new_raw_run_properties.sql','w')
for line in lines:
    arr = line.split(",")
    arr[11] = '"%s"' % arr[11]
    newline = ','.join(arr)
    ofile.write(newline)
ofile.close()

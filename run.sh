#!/bin/bash

##
## Startup script to run the pipeline for the Florida NAL rercords
##
##  The script ensures the module search path includes the source
##  dir.
##

export PYTHONPATH='./src/':$PYTHONPATH
python3 ./src/driver.py $1 

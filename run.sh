#!/bin/bash

##
## Startup script to run the pipeline for the Florida NAL rercords
## It takes an optional config label argument, to let us
## toggle config for the remote (EC2) instance.
##
##  The script ensures the module search path includes the source
##  dir.
##

export PYTHONPATH='./src/':$PYTHONPATH
python3 ./src/driver.py $1 

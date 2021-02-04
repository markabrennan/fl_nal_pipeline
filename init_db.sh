#!/bin/bash

##
## Script to run a CREATE statement against the configured db
## It takes an optional config label argument, to let us
## toggle config for the remote (EC2) instance.
##
##  The script ensures the module search path includes the source
##  dir.
##

export PYTHONPATH='./src/':$PYTHONPATH
python3 ./src/db_tools.py $1

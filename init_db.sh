#!/bin/bash

##
## Script to run a CREATE statement against the configured db
##
##  The script ensures the module search path includes the source
##  dir.
##

export PYTHONPATH='./src/':$PYTHONPATH
python3 ./src/db_tools.py

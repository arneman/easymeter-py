#!/bin/bash
ABSPATH=$(readlink -f $0)
ABSDIR=$(dirname $ABSPATH)

cd $ABSDIR
py-env/bin/python3 main.py
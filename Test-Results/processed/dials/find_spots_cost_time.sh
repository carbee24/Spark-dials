#!/bin/bash

start=`date +%s`

dials.find_spots imported.expt  nproc=$1

end=`date +%s`

cost=$[ end - start ]

echo "Find spots cost : "

echo $cost

###   proc   time
###   90     125
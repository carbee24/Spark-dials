#!/bin/bash

start=`date +%s`

dials.integrate refined.expt refined.refl nproc=$1

end=`date +%s`

cost=$[ end - start ]

echo "Integration cost : "

echo $cost

###   proc   time
###   90     349
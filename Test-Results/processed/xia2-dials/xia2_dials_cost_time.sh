#!/bin/bash

start=`date +%s`

xia2 pipeline=dials  nproc=90 image=/home/Data/distributed_computing_test/Tau-nat/Tau-natA5_1_master.h5

end=`date +%s`

cost=$[ end - start ]

echo "xia2 cost : " 

echo $cost

###   proc   time
###   90     811
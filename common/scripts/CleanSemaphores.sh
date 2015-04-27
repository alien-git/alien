#!/bin/bash

# get list of semaphores
sems=$(ipcs -s | awk --source '/0x0*.*[0-9]* .*/ { print $2 }')

# loop over list and check if owner-process is still alive
for sem in $sems; do
	echo "Checking semaphore $sem ..."
	
	# get pid of owner-process
	pid=`ipcs -s -i $sem | awk '/./{line=$0} END { print line }' | awk '{ print $5 }'`
	
	if [ -d /proc/$pid ]; then
		echo "Process with PID $pid exists"
		echo "semaphore is not deleted"
	else
		echo "$pid does not exist"
		echo "semaphore is deleted"
		ipcrm -s $sem
	fi

done

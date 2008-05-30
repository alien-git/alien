#!/bin/bash
#
# ServiceStatus.sh Checks all services, output parsed by MonaLisa
#

ERRCODE=0
ERRMSG=""
ETCDIR=$HOME/.alien/etc/aliend

if [ -r $ETCDIR/startup.conf ] ; then
	. $ETCDIR/startup.conf
	ALL_ORG=$ALIEN_ORGANISATIONS
else
	ALL_ORG="ALICE"
	ERRCODE=2
	ERRMSG="Global $ETCDIR/startup.conf not found, assuming ALICE."
fi

if [ -n "$1" ] ; then
	ALL_ORG="$*"
	ERRCODE=0
	ERRMSG="Using parameters: $*"
fi

for ALIEN_ORGANISATION in $ALL_ORG ; do
	if [ -r $ETCDIR/$ALIEN_ORGANISATION/startup.conf ] ; then
		. $ETCDIR/$ALIEN_ORGANISATION/startup.conf
	else
		MonitorServices="Monitor CE SE PackMan MonaLisa"
		ERRCODE=2
		ERRMSG="$ERRMSG Cannot read $ETCDIR/$ALIEN_ORGANISATION/startup.conf, assuming default services."
	fi
	AliEnCommand="$ALIEN_ROOT/bin/alien"
	MonitorServices=${MonitorServices:-$AliEnServices}
	for service in $MonitorServices ; do
		err_msg=`$AliEnCommand --org $ALIEN_ORGANISATION Status$service -silent 2>&1`
		error=$?
		err_msg=`echo $err_msg | while read line ; do echo -n \$line | sed -e "s/\t/  /g"; done`
		if [ "$error" != "0" ] ; then
			if [ ! -x $AliEnCommand ] ; then
				echo -e "$service\tStatus\t$error\tMessage\tCannot execute $AliEnCommand"
			else
				echo -e "$service\tStatus\t$error\tMessage\t$err_msg. Exit code $error"
			fi
		else
			echo -e "$service\tStatus\t$error"
		fi
	done
done
if [ -n "$ERRMSG" ] ; then
	echo -e "SCRIPTRESULT\tStatus\t$ERRCODE\tMessage\t$ERRMSG"
else
	echo -e "SCRIPTRESULT\tStatus\t$ERRCODE"
fi


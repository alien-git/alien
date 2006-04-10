#!/bin/sh
#
# ServiceStatus.sh Checks all services, output parsed by MonaLisa
#

ETCDIR=$HOME/.alien/etc/aliend
. $ETCDIR/startup.conf

ALL_ORG=$ALIEN_ORGANISATIONS

if [ -n "$1" ] ;
then
   ALL_ORG="$*"
fi

ERRCODE=0

for ALIEN_ORGANISATION in $ALL_ORG
do
    if [ -f $ETCDIR/$ALIEN_ORGANISATION/startup.conf ];
    then
      . $ETCDIR/$ALIEN_ORGANISATION/startup.conf
      for service in $AliEnServices
      do
        $AliEnCommand --org $ALIEN_ORGANISATION Status$service -silent > /dev/null 2>&1
        error=$?
        echo -e "$service\tStatus\t$error"
      done
    else
      ERRMSG="File $ETCDIR/$ALIEN_ORGANISATION/startup.conf does not exist!"
      ERRCODE=1
    fi
done

echo
echo -e "SCRIPTRESULT\tStatus\t$ERRCODE\tMessage\t$ERRMSG"



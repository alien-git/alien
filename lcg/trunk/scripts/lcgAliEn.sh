#!/bin/bash

AliEnCommand=$VO_ALICE_SW_DIR/alien/bin/alien

if [ "$1" = '-s' ] ;
then
 message='Setting the service proxy '
 shift
 if [ -f /etc/rc.d/init.d/functions ]
 then
    OLDPATH=$PATH
    . /etc/rc.d/init.d/functions
    PATH=$OLDPATH
 else
    echo_success()
    {
        echo -n "                                       ok"
    }
    echo_failure()
    {
        echo -n "                                       failed"
    }
 fi
else
 message=''
 echo_success()
 {
     echo -n
 }
 echo_failure()
 {
     echo -n "Error setting proxy."
 }
fi
echo -n "$message"
# Always take the first one (may not be the right option...)
proxy=`vobox-proxy --vo alice --dn all query | grep File: | cut -d' ' -f2 | head -1`
  error=$?
  if [ $error -ne 0 ] || [ "$proxy" == '' ] ;
  then
     echo_failure
     echo
     exit 3
  fi
  export X509_USER_PROXY=$proxy 
  echo_success
  echo
  $AliEnCommand $*

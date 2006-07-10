#!/bin/bash

AliEnCommand=$VO_ALICE_SW_DIR/alien/bin/alien

uid=`$AliEnCommand --printenv | grep ALIEN_USER | cut --d='='  -f2`
host=`$AliEnCommand -user aliprod --exec echo LDAPHOST | cut -d\' -f2`
dn=`ldapsearch -x -LLL -H ldap://$host -b uid=$uid,ou=people,o=alice,dc=cern,dc=ch subject| perl -p -00 -e 's/\n //' | grep -v 'dn:'| sed 's/subject: //'`
proxy=`vobox-proxy --vo alice -dn "$dn" query-proxy-filename`
error=$?
if [ $error -ne 0 ] || [ "$proxy" == '' ] ;
then
   echo "Error setting proxy" 1>&2
   exit 3
fi
env X509_USER_PROXY=$proxy $AliEnCommand $*

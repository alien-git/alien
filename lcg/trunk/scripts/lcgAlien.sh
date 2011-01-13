#!/bin/bash

AliEnCommand=$VO_ALICE_SW_DIR/alien/bin/alien

uid=`$AliEnCommand --printenv | grep ALIEN_USER | cut --d='='  -f2`
host=`$AliEnCommand -user aliprod --exec echo LDAPHOST | cut -d\' -f2`
dns=`ldapsearch -x -LLL -H ldap://$host -b uid=$uid,ou=people,o=alice,dc=cern,dc=ch subject| perl -p -00 -e 's/\n\n/\n/g;s/^dn:.*\n//g;s/\n //g;s/subject: //g;s/ /\#\#\#/g'`
for line in $dns
do
  dn=`echo $line|sed -e 's/###/ /g'`
  echo "Trying $dn";
  proxy=`vobox-proxy --vo alice -dn "$dn" query-proxy-filename`
  error=$?
  if [ $error -eq 0 ] 
  then
    env X509_USER_PROXY=$proxy $AliEnCommand $*
    exit 0
  fi
done
echo "Error setting proxy" 1>&2
exit 3

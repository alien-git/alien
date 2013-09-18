#!/bin/bash

for d in $ALIEN_ROOT ~ ~/alien $VO_ALICE_SW_DIR/alien
do
    AliEnCommand=$d/bin/alien

    [ -x $AliEnCommand ] && break
done

user=`$AliEnCommand --printenv | awk -F= '$1 == "ALIEN_USER" { print $2 }'`

fatal()
{
    echo "${@-unspecified error}" >&2
    exit 3
}

[ "X$user" = X ] && fatal "Cannot determine the AliEn user"

host=`
    $AliEnCommand -user aliprod --exec echo LDAPHOST 2>&1 |
    sed -n "s/'//g;s/.*\<LDAPHOST\> *= *//p"
`

[ "X$host" = X ] && host=alice-ldap.cern.ch:8389

for dnq in `
	ldapsearch -x -LLL -H ldap://$host -b \
	    uid=$user,ou=people,o=alice,dc=cern,dc=ch subject |
	    perl -p00e 's/\n //g' |
	    perl -ne 's/ /?/g; print if s/^subject:\?*//i'
    `
do
    dn=${dnq//\?/ }
    echo "Trying $dn"

    proxy=`
	vobox-proxy --vo alice --voms alice:/alice/Role=lcgadmin \
	    --dn "$dn" query-proxy-filename 2> /dev/null
    `

    if [ $? = 0 ] && [ -f "$proxy" ]
    then
	X509_USER_PROXY=$proxy exec $AliEnCommand "$@"
	exit
    fi
done

fatal "Could not find the correct proxy"

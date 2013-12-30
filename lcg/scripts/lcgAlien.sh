#!/bin/bash

for d in $MY_ALIEN $ALIEN_ROOT ~ /cvmfs/alice.cern.ch
do
    AliEnCommand=$d/bin/alien

    [ -x $AliEnCommand ] && break
done

user=`$AliEnCommand --printenv | grep ^ALIEN_USER=`

fatal()
{
    echo "${@-unspecified error}" >&2
    exit 3
}

[ "X$user" = X ] && fatal "Cannot determine the AliEn user"

export $user

host=`
    $AliEnCommand --exec echo LDAPHOST 2>&1 |
    sed -n "s/'//g;s/.*\<LDAPHOST\> *= *//p"
`

[ "X$host" = X ] && host=alice-ldap.cern.ch:8389

for dnq in `
	ldapsearch -x -LLL -H ldap://$host -b \
	    uid=$ALIEN_USER,ou=people,o=alice,dc=cern,dc=ch subject |
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

    [ $? = 0 ] && [ -f "$proxy" ] || continue

    timeleft=`X509_USER_PROXY=$proxy vobox-proxy query-proxy-timeleft`

    [ "X$timeleft" != X ] || continue
    
    let 'timeleft /= 3600'
    thr=40

    [ $timeleft -gt $thr ] || {
	echo "Warning - proxy lifetime is low: $timeleft < $thr hours"
    }

    [ $timeleft -gt 0 ] || continue

    X509_USER_PROXY=$proxy exec $AliEnCommand "$@"
    exit
done

fatal "Could not find the correct proxy"

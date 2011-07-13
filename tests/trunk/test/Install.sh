#!/bin/bash
#########################################################################
ALIEN_INSTALL()
{
    echo "DOING INSTALL";
    if  [ "$EGEE_VERSION" == 1 ] ;
	then 
	  echo "Getting the version from EGEE"
	  DATE=`date +%Y%m%d`
	  NAME="-2.0.0-1_N${DATE}.i386.rpm"
          RPMS="admin${NAME} classad${NAME} common${NAME} gas${NAME} gssapi${NAME} perl${NAME} srm${NAME} rls${NAME}";
    else
	NAME="${VERSION}.i386.rpm"
	RPMS="AliEn-Base-${NAME} AliEn-Admin-${NAME} AliEn-CE-${NAME} AliEn-Client-${NAME} AliEn-GUI-${NAME} AliEn-SE-${NAME} AliEn-Monitor-${NAME}"
    fi

    ALIEN_STOP_ROOT_SERVICES

    COMMENTS[0]="Deleting the old installation ($ALIEN_ROOT)"

    COMMANDS[0]="rm -rf ${ALIEN_ROOT}"

    EXECUTE_SHELL

    ALIEN_GET_RPMS  && exit     

    rm -rf /etc/aliend/E*

    COMMENTS[0]="Bootstraping alien"
    COMMENTS[1]="Making alienmaster the owner of the directory"
    COMMENTS[2]="Copying the host certificate"


    COMMANDS[0]="echo;${ALIEN_ROOT}/bin/alien-perl --bootstrap"
    COMMANDS[1]="chown -R alienmaster ${ALIEN_ROOT}"
    mkdir -p /etc/aliend/ldap
    COMMANDS[2]="cp /home/alienmaster/certs/ /etc/aliend/ldap -R"

    EXECUTE_SHELL

}
#########################################################################
ALIEN_GET_RPMS()
{
  printf "%-60s" "Creating a temporary directory for the rpms..."
  TMP_DIR="/tmp/alien.install.$$"
  mkdir $TMP_DIR
  [ $? -ne 0 ] && echo "error!!" && return 
  echo ok
  OLDIR=`pwd`
  cd  $TMP_DIR 

  echo "Getting the rpms..."
  for RPM in $RPMS
  do
    printf "%-60s" "     Getting $RPM..." 

		if  [ "$EGEE_VERSION" == 1 ] ;
		then 
		  DATE=`date +%Y%m%d`
      ADDRESS="http://glite.web.cern.ch/glite/packages/N${DATE}/bin/rhel30/i386/RPMS/glite-alien-$RPM"
		else 
      ADDRESS="http://alien.cern.ch/dist/${VERSION}/${PLATFORM}/RPMS/${RPM}"
		fi
    wget $ADDRESS -q -O ${TMP_DIR}/$RPM
    [ $? -ne 0 ] && echo "error getting  $ADDRESS -q -O ${TMP_DIR}/$RPM!!" && return 
    echo ok
  done 
  echo "DONE"

  COMMENTS[0]="Making a copy of the rpm's database"

    COMMANDS[0]="mkdir -p ${ALIEN_ROOT}"

    COMMENTS[1]="Copying RPM database"
    COMMANDS[1]="[ -d ${ALIEN_ROOT}/rpmdb ] ||  cp -dR /var/lib/rpm  ${ALIEN_ROOT}/rpmdb" 

    
    COMMENTS[2]="Installing the rpms"
    COMMANDS[2]="echo;rpm -Uvh --prefix ${ALIEN_ROOT} --dbpath ${ALIEN_ROOT}/rpmdb ${TMP_DIR}/*.rpm --force --nodeps"

    COMMENTS[3]="Removing the temporary directory"
    COMMANDS[3]="rm -rf $TMP_DIR"


    EXECUTE_SHELL

    cd $OLDDIR

    return 1
}

ALIEN_PORTAL()
{
    echo "Testing the Portal";
    if [ "$INSTALL_PORTAL" == "1" ] ; 
    then
	NAME="${VERSION}.i386.rpm"
	
	RPMS="AliEn-Portal-${NAME}";
	
	ALIEN_GET_RPMS && exit


	printf "%-60s" "Copying the host certificate" 
	cp -R /home/alienmaster/certs/  \
	    /home/alienmaster/AliEn/Admin/apache/conf/httpd.conf \
	    ${ALIEN_ROOT}/apache/conf -R || echo "error!"
	echo ok
	echo "Making alienmaster the owner of the directory"
	chown -R alienmaster ${ALIEN_ROOT}

    fi
    echo "Starting the tests"
    TESTS="42-startTail 38-imlib 28-image 27-map 24-createorgportal 25-webpages 29-piechart 39-Jobwebpage 43-stopTail "
    ALIEN_TESTS
}
ALIEN_STOP_ROOT_SERVICES()
{
    echo "Trying to stop all the services"
    FILE="/tmp/alien.stop.$$"
    if [[ -f ${ALIEN_ROOT}/bin/alien ]] ;
    then
	for SERVICE in "SE" "Monitor"
	do  
	    echo "Stopping $SERVICE"
	    ${ALIEN_ROOT}/bin/alien Stop$SERVICE >$FILE  2>&1
	done
    fi

    for SERVICE in "aliend" "alien-mysqld" "alien-ldap" 
    do
	echo "Stopping $SERVICE"
	[[ -f ${ALIEN_ROOT}/etc/rc.d/init.d/$SERVICE  ]] &&  \
		${ALIEN_ROOT}/etc/rc.d/init.d/$SERVICE stop >$FILE  2>&1
    done


    echo "Checking if there are any processes left..."
    PROCESSES=`ps -Ao command |grep AliE |grep -v grep `

    echo "KILLING $PROCESSES (I am $$)"
    PROCESSES=`ps -Ao command |grep AliE |grep -v $$ |awk '{print $2}'`
    if  [[ -n "$PROCESSES" ]] ;
    then
	echo "KILLING $PROCESSES"
	kill -9 $PROCESSES
    fi
    rm -rf $FILE
}
#########################################################################
ALIEN_ROOT_TESTS()
{
    #TEST TO DO AS ROOT
    ALIEN_STOP_ROOT_SERVICES

    TESTS="42-startTail 01-use 02-classads 04-createorgldap 03-createorgdb 61-rotate 05-createorgservices 49-uninitialized 43-stopTail"
    ALIEN_TESTS

    org=`hostname -s`
    echo "Copying the ssh key of alienmaster (in ${org})"
    mkdir -p /root/.alien
    rm -rf /root/.alien/identities.${org}
    cp -rf /home/alienmaster/.alien/identities.${org}  /root/.alien
    cp -rf /home/alienmaster/.alien/Environment  /root/.alien
    echo "export ALIEN_USER=alienmaster">>/root/.alien/Environment
#    exit

}

ALIEN_SLAVE_TESTS()
{
    echo "Let's do all the tests of the slave"

}

ALIEN_SLAVE_INSTALL() 
{
    echo "First, we have to install AliEn in the slave"
    dir="/home/alienmaster/AliEn"
    COMMENTS[0]="Removing $dir in $SLAVE"
    COMMENTS[1]="Creating $dir"
    COMMENTS[2]="Copying $dir"
    COMMENTS[3]="Making alienmaster the owner of the directory"
    COMMENTS[4]="Installing the AliEn RPMS in $SLAVE"

    COMMANDS[0]="ssh $SLAVE rm -rf $dir"
    COMMANDS[1]="ssh $SLAVE mkdir  $dir"
    COMMANDS[2]="scp -qr $dir/t  ${SLAVE}:$dir"
    COMMANDS[3]="ssh $SLAVE chown alienmaster $dir"
    COMMANDS[4]="ssh $SLAVE $dir/t/Install.sh -install"

    EXECUTE_SHELL

}
RUN_TEST()
{
    i=$1
    TEST=${2%%.t}

	OUTPUT="$DIR/$i.test.${TEST/\//.}"
    shift 3

    LINE2="$TEST........................................................................"
    printf "Test %2i ${LINE2:0:60}" $i
    number=${TEST%%-*}

    if [ "$number" == "$BEGIN" ]
	then
	ACTIVE=1
    fi

    if [ "$IGNORE" != "${IGNORE/$number//}" ] || [ $ACTIVE -ne 1 ]
    then
	echo ignored
	return 0
    fi
    
    rm -rf $DIR/current
    ln -s $OUTPUT $DIR/current

    START=`date +"%s"`
    INPUTFILE="$DIR/${TEST/\//.}.input"
    INPUT=""
    [ -f $INPUTFILE ] && INPUT=`grep '^#ALIEN_OUTPUT' $INPUTFILE |sed -e 's/#ALIEN_OUTPUT //'`
    $ALIEN -x $ALIEN_TESTDIR/${TEST}.t $INPUT >$OUTPUT 2>&1
    #cat $OUTPUT
    DONE=$?
    grep '^#ALIEN_OUTPUT' $OUTPUT >$DIR/${TEST/\//.}.b.input
    END=`date +"%s"`
    let TIME=$END-$START

#	    $ALIEN proxy-info > /dev/null 2>&1  && echo -n "WARNING THERE IS A PROXY"
    [ -f "$CAFILE"  ] ||  echo -n "WARNING THE CA $CAFILE IS NOT IN X509_CERT_DIR"
    printf "(%-4s seconds)" $TIME
    grep "Use of uninitialized value" $OUTPUT  && DONE=22
    grep "masks earlier declaration in same scope" $OUTPUT  && DONE=22
    grep "used only once: possible typo " $OUTPUT && DONE=22
    grep "Useless use of " $OUTPUT  && DONE=22
    grep "not ok " $OUTPUT && DONE=22
            
# 	ERROR=`grep -i error $OUTPUT`
    [ -z "$APMON" ] || SEND_TO_ML "${TEST}_time" $TIME

    if [  $DONE -ne 0  ] || [  -n "$ERROR"  ] ;
    then
	echo -n failed
	if [ "$EXPECTED_FAILS" != "${EXPECTED_FAILS/,$number,/,}" ]
	then
	    echo -n "(expected)"
	    EXPECTED_FAILS=${EXPECTED_FAILS/,$number,/,}
	else 
	    let FAILEDTESTS++
	    if [ "$NO_BREAK" != "1" ] ;
	    then
		nSuccess=`expr $nTests - $FAILEDTESTS`
		SEND_TO_ML "${FUNCTION}_nTests" $nTests \
		"${FUNCTION}_nSuccess" $nSuccess \
		"${FUNCTION}_pSuccess" `expr $nSuccess \* 100 / $nTests` \
		"${FUNCTION}_time" `expr $END - $allStart`
		echo; echo;echo
		echo "Error doing $TEST $i" 
		echo "Output"
		cat $OUTPUT
		exit -2
	    fi
	fi
	echo
    else
	echo "ok"
	rm -f $OUTPUT
    fi
}
ALIEN_TESTS()
{
    ALLSTART=`date +"%s"`
    echo "Checking if the certificate is ok"
    export PATH=$ALIEN_ROOT/bin:$PATH
    export LD_LIBRARY_PATH=$ALIEN_ROOT/lib:$LD_LIBRARY_PATH
    export DYLD_LIBRARY_PATH=$ALIEN_ROOT/lib:$DYLD_LIBRARY_PATH
    openssl verify -CApath $ALIEN_ROOT/globus/share/certificates -purpose sslclient $HOME/.alien/globus/usercert.pem

    i=1

    ACTIVE=1
    if [ "$BEGIN" != "" ]
    then
    	ACTIVE=0
    fi
    BASEDIR="/tmp/alien_tests";
    DIR="$BASEDIR/`whoami`/"
    CURRENT="$DIR/current"
    DIR="$DIR/$$/"

    [ -d $BASEDIR ] || ( mkdir -p $BASEDIR; chmod o+w $BASEDIR )   # chmod is important if other users run tests on this machine as well
    [ -d $DIR ] || mkdir -p $DIR

    [ -e $CURRENT  ] && rm -f  $CURRENT

    ln -s $DIR  $CURRENT 

    if [ $USER = "root" ]
    then
	echo "RUNNING THE TESTS AS ROOT. Including tail"
	TESTS="42-startTail $TESTS 43-stopTail"
    fi
    nTests=`echo "$TESTS" | wc -w`
    allStart=`date +"%s"`
    FAILEDTESTS=0
    CAFILE=$ALIEN_ROOT/globus/share/certificates/`openssl x509 -hash -noout < $HOME/.alien/globus/cacert.pem`.0
    for TEST in $TESTS
    do
	  RUN_TEST $i $TEST
	  let i++
    done


    for TEST in $TESTS
    do
	if [ -f "$ALIEN_TESTDIR/${TEST%%.t}.b.t" ];
	then
	    RUN_TEST $i ${TEST%%.t}.b 
	    let i++
	fi
    done
    allEnd=`date +"%s"`
    nSuccess=`expr $nTests - $FAILEDTESTS`
    SEND_TO_ML "${FUNCTION}_nTests" $nTests \
		"${FUNCTION}_nSuccess" $nSuccess \
		"${FUNCTION}_pSuccess" `expr $nSuccess \* 100 / $nTests` \
		"${FUNCTION}_time" `expr $allEnd - $allStart`
    ALLEND=`date +"%s"`
    let ALLTIME=$ALLEND-$ALLSTART
    echo "In total, there are $FAILEDTESTS tests that failed (there were supposed to be $EXPECTED_FAILURES). It took $ALLTIME seconds"
    if [ "$EXPECTED_FAILURES" != "$FAILEDTESTS" ] ;
    then
	echo "NOPE!!"
	exit -2
    fi
    if [ "${EXPECTED_FAILS//,/}" ];
    then
	echo "Tests $EXPECTED_FAILS were supposed to fail (and didn't)"
	exit -2
    fi

}

# Send a set of parameters to ML, for BITS_AliEn_Tests/$platform cluster/node
SEND_TO_ML()
{
    cluster='BITS_AliEn_Tests'
    node=`uname -m`
    params="\"$cluster\", \"$node\""
    until [ -z "$1" -a -z "$2" ]
    do
	params="$params, \"$1\", $2"
	shift; shift
    done
    #echo "Params = [$params]"
    destination="['aliendb5.cern.ch:8884']"
    exe="use strict;
use warnings;
use ApMon;
ApMon::Common::setLogLevel('WARNING');
my \$apm = new ApMon(0);
\$apm->setDestinations($destination);
\$apm->sendParameters($params);"
    echo $exe | ${ALIEN}-perl
}

ALIEN_CREATE_CERT()
{
    echo "Creating a self signed certificate to run the tests"

    export PATH=$ALIEN_ROOT/bin:$PATH
    export LD_LIBRARY_PATH=$ALIEN_ROOT/lib:$LD_LIBRARY_PATH
    export DYLD_LIBRARY_PATH=$ALIEN_ROOT/lib:$DYLD_LIBRARY_PATH

    USERDIR=$HOME/.alien/globus
    DIR=$HOME/.alien/etc/aliend/ldap/certs/
    CA_KEY=$HOME/.alien/globus/cakey.pem
    CA_CERT=$HOME/.alien/globus/cacert.pem

    HOST_KEY=$DIR/host.key.pem
    HOST_CERT=$DIR/host.cert.pem
    COMMENTS[0]="Creating the directory for the certificate"
    COMMENTS[1]="Creating the certificate"
    COMMENTS[2]="Changing the privileges"
    COMMENTS[3]="Creating a self signed CA certificate"
    COMMENTS[4]="Changing the privileges"
    COMMENTS[5]="Creating CA file"
    COMMENTS[6]="Using $HOME/.globus"
    COMMENTS[7]="Creating user certificate request"
    COMMENTS[8]="Signing user certificate with CA key"
    COMMENTS[9]="Changing permissions of user certificate and key"
    COMMENTS[10]="Copying usercert.pem to host.cert.pem"
    COMMENTS[11]="Copying userkey.pem to host.key.pem"
    COMMENTS[12]="Running alien config"

    COMMANDS[0]="mkdir -p $DIR $USERDIR"
    COMMANDS[1]="openssl genrsa -out $CA_KEY 1024 > /dev/null 2>&1"
    COMMANDS[2]="chmod go-rwx $CA_KEY"
    COMMANDS[3]="openssl req -new -batch -key $CA_KEY -x509 -days 365 -out $CA_CERT  -subj \"/C=CH/O=AliEn/CN=AlienCA\" "
    COMMANDS[4]="chmod go-rwx $CA_CERT"
    COMMANDS[5]="cp $CA_CERT $ALIEN_ROOT/globus/share/certificates/\`openssl x509 -hash -noout < $USERDIR/cacert.pem\`.0"
    COMMANDS[6]="rm -rf $HOME/.globus;ln -s  $USERDIR $HOME/.globus"
    COMMANDS[7]="openssl req -nodes -newkey rsa:1024 -out $USERDIR/userreq.pem -keyout $USERDIR/userkey.pem -subj \"/C=CH/O=AliEn/CN=test user cert\" >/dev/null 2>&1"
    COMMANDS[8]="openssl x509 -req -in $USERDIR/userreq.pem -CA $CA_CERT -CAkey $CA_KEY -CAcreateserial -out $USERDIR/usercert.pem >/dev/null 2>&1"
    COMMANDS[9]="chmod 644 $USERDIR/usercert.pem;chmod 600 $USERDIR/userkey.pem"
    COMMANDS[10]="cp -f $USERDIR/usercert.pem $HOST_CERT"
    COMMANDS[11]="cp -f $USERDIR/userkey.pem $HOST_KEY"
    COMMANDS[12]="$ALIEN_ROOT/bin/alien config "

    which openssl
    EXECUTE_SHELL "CREATE_CERT"

    CAFILE=$ALIEN_ROOT/globus/share/certificates/`openssl x509 -hash -noout < $CA_CERT`.0
    SIGNING_POLICY_FILE=$ALIEN_ROOT/globus/share/certificates/`openssl x509 -in $CA_CERT -noout -hash`.signing_policy
    SUBJECT=`openssl x509 -subject -noout < $CA_CERT|awk -F' ' '{print $2}'`

    echo "Let's create siging policy file"
    echo "access_id_CA X509 '$SUBJECT'" >  $SIGNING_POLICY_FILE
    echo "pos_rights globus CA:sign" >> $SIGNING_POLICY_FILE
    echo "cond_subjects globus '*'" >> $SIGNING_POLICY_FILE


    echo "Let's check if the file $CAFILE exists"
    [ -f "$CAFILE"  ] ||  echo -n "WARNING THE CA IS NOT IN X509_CERT_DIR"
    echo "Checking if the certificate is ok"
    openssl verify -CApath $ALIEN_ROOT/globus/share/certificates -purpose sslclient $USERDIR/usercert.pem
    exit;

}
ALIEN_NEW_MACHINE()
{
    echo "Setting up the machine for running the tests"

    userdel alienmaster >/dev/null 2>&1
    userdel newuser >/dev/null 2>&1

    COMMENTS[0]="Adding the user 'alienmaster'"
    COMMENTS[1]="Logging to cvs"
    COMMENTS[2]="Copying the cvs to alienmaster"
    COMMENTS[3]="Check out AliEn"

    COMMANDS[0]="adduser alienmaster"
    COMMANDS[1]="echo \":pserver:cvs@alisoft.cern.ch:/soft/cvsroot Ah<Z\" >~/.cvspass"
    COMMANDS[2]="cp /root/.cvspass /home/alienmaster/"
    COMMANDS[3]="cd /home/alienmaster/;cvs -d :pserver:cvs@alisoft.cern.ch:/soft/cvsroot co AliEn >/dev/null 2>&1"

    
    COMMENTS[4]="Checking if there is a certificate in /home/alienmaster/certs"
    COMMANDS[4]="ls  /home/alienmaster/certs/host.cert.pem >/dev/null"


    COMMENTS[5]="Putting the certificate in /root/.alien/globus"
    COMMANDS[5]="mkdir -p /root/.alien/globus;cp /home/alienmaster/certs/host.cert.pem /root/.alien/globus/usercert.pem && cp /home/alienmaster/certs/host.key.pem /root/.alien/globus/userkey.pem; chmod 0600 /root/.alien/globus/userkey.pem"

    COMMENTS[6]="Adding user 'newuser'"
    COMMANDS[6]="adduser newuser -s /bin/false -p AA4yjtXbtR2IM"



    EXECUTE_SHELL "NEW_MACHINE"
    exit;

}
EXECUTE_SHELL()
{
    nCmds=${#COMMANDS[@]}
    group=$1
    startTime=`date +"%s"`
    for (( i=0; i<$nCmds; i++)) ;
	do
	 printf "%-60s" " $i: ${COMMENTS[$i]}  ... "
	 eval  ${COMMANDS[$i]}

         EXITCODE=$?
	 endTime=`date +"%s"`
	[ $EXITCODE -ne 0 ] && echo "error!!" && SEND_TO_ML "${group}_nTests" $nCmds "${group}_nSuccess" $i "${group}_pSuccess" `expr $i \* 100 / $nCmds` "${group}_time" `expr $endTime - $startTime` && exit -1 
	echo ok
    done
    endTime=`date +"%s"`
    SEND_TO_ML "${group}_nTests" $nCmds "${group}_nSuccess" $nCmds "${group}_pSuccess" 100 "${group}_time" `expr $endTime - $startTime`
}

D=`dirname $0`;
if [ -d "$ALIEN_TESTDIR" ];
then
  D=$ALIEN_TESTDIR
fi
cd $D
DIRS=`find .  -maxdepth 1 -type d | grep -v CVS | grep -v ".svn" |grep ./ | awk -F / '{print $2}'`
for DIR in $DIRS; do
  var=`echo $DIR | tr '[:lower:]' '[:upper:]'`_TESTS_LIST 
  value=`find $DIR -type f -not -name "*.b.t" |grep -v CVS | grep -v ".svn" | sort `
  export $var="$value"
done;
echo "VAMOS BIEN"
JOB_TESTS_LIST="$JOB_MANUAL_TESTS_LIST $JOB_AUTOMATIC_TESTS_LIST"
PACKAGE_TESTS_LIST="$PACKAGES_TESTS_LIST job_automatic/025-executeAllJobs"
USER_TESTS_LIST="$USER_BASIC_TESTS_LIST $CATALOGUE_TESTS_LIST $FILE_QUOTA_TESTS_LIST $TRANSFERS_TESTS_LIST $JOB_TESTS_LIST $PACKAGE_TESTS_LIST 68-dbthreads"
CVS_INSTALL_LIST="10-cvs-co "

NEW_VO_LIST="user_basic/001-use $NEW_VO_TESTS_LIST"

GET_ARGUMENTS()
{
    [ -n "$*"   ] &&  echo "Parsing the arguments $*" 
    ERROR=""
    while [ $# -gt 0 ]
     do
	case $1 in
	    -INSTALL|-install) 
		INSTALL=1
		;;
	    -CVS_INSTALL|-cvs_install)
	        CVS_INSTALL=1
		;;
	    -ROOT_TESTS|-root_tests)
	        ROOT_TESTS=1
		;;
	    -USER_TESTS|-user_tests)
	        USER_TESTS=1
		;;
	    -GAS_TESTS|-gas_tests)
	        GAS_TESTS=1
		;;
	    -GAPI_TESTS|-gapi_tests)
		GAPI_TESTS=1
		;;
            -BANK-TESTS|-bank_tests)
		BANK_TESTS=1
		;;
	    -NEW_VO|-new_vo)
		mv  $HOME/.alien/Environment $HOME/.alien/Environment_test
		echo export ALIEN_IGNORE_BLOCK=1>  $HOME/.alien/Environment
	        NEW_VO=1
		;;
	    -CATALOGUE_TESTS|-catalogue_tests)
	        CATALOGUE_TESTS=1
		;;
	    -JOB_TESTS|-job_tests)
	        JOB_TESTS=1
		;;
	    -PACKAGE_TESTS|-package_tests)
	        PACKAGE_TESTS=1
		;;
	    -TRANSFERS_TESTS|-transfers_tests)
	        TRANSFERS_TESTS=1
		;;
		-NO_BREAK|-no_break)
	        NO_BREAK=1
		;;
	    -PORTAL|-portal)
	        PORTAL=1
		;;
	    -INSTALL_PORTAL|-install_portal)
	        INSTALL_PORTAL=1
		;;
		-job_manual|-JOB_MANUAL)
		    JOB_MANUAL_TESTS=1
		;;
	    -IGNORE|-ignore)
		shift 1
		IGNORE=$1
		;;
	    -BEGIN|-begin)
	    	shift 1
		BEGIN=$1
	    	;;
	    -PLATFORM|-platform)
		shift 1
		PLATFORM=$1
		;;
	    -ORGANISATION|-organisation|-org|-ORG)
		shift 1
		ALIEN_ORGANISATION=$1
		;;
	    -VERSION|-version)
		shift 1
		VERSION=$1
		;;
	    -ALL|-all)
		ALL=1
		;;
	    -FAIL|-fail)
		shift 1
		EXPECTED_FAILURES=$1
		;;
	    -EXPECTED|-expected)
		shift 1
		EXPECTED_FAILS=",$1,"
		;;
	    -PERFORMANCE_TESTS|-performance_tests)
		PERFORMANCE_TESTS=1
		;;
	    -NEW_MACHINE|-new_machine)
		NEW_MACHINE=1
		;;
	    -CREATE_CERT|-create_cert)
		CREATE_CERT=1
		;;
	    -SLAVE_INSTALL|-slave_install)
		SLAVE_INSTALL=1
		;;
	    -SLAVE_TESTS|-slave_tests)
		SLAVE_TESTS=1
		;;
	    -SLAVE|-slave)
		shift 1
		SLAVE=$1
		if [ -z "$SLAVE" ]
 		then
		    ERROR=" argument slave takes one parameter";
		fi
		
		;;
	    -APMON|-apmon)
		APMON=1
		;;
	    *)
		ERROR=" I don't understand argument '$1' :( ..."
		;;
	esac
	if  [ "$ERROR" !=  "" ] 
	then 
	    echo "Error! $ERROR "
	    echo "Usage:"
	    echo "      $0 [-install] [-cvs_install] [-root_tests] [-no_user_tests] [-no_break] [-ignore <test no>[,<testno>]*] [-platform <name>] [-install_portal] [-organisation <org_name>] [-version <version] [-all] [-new_machine] [-slave <name> [-slave_install] [-slave_tests]]"
	    echo
	    echo "Options:
install -> It will delete the local installation, take the RPMS from the alien web server  and reinstall everything

root_tests -> It will create a new VO (you have to be root to be able to  run these tests)

user_tests-> Tests connecting to the VO created during the root_tests

platform -> i386-linux24 (default) or i386-linux22. AliEn will be installed in /opt/alien.<platform>, so you can have several platforms in the same machine.

install_portal -> Installs the RPM of the portal and creates the portal for the VO

no_break -> By default, as soon as a test fails, the script stops. If 'no_break' is specified, it will continue with the rest of the tests.

ignore -> Ignore a test (you have to specify the test number)

version -> Specify version of AliEn to install (default ${VERSION})

all -> Make all the possible tests
		1. install + root_tests + user_tests
		2. install_portal + portal
		3. cvs_install + root_tests + user_tests

new_machine -> Creates necessary users to run all the tests in a new machine

slave_install -> Install AliEn rpms in another machine (it requires the -slave parameter!)

slave_tests  -> Test transfers from another machine

"
	    exit 1
	fi
	shift 1
    done
    if [ "$SLAVE_INSTALL" ==  "1" ]  || [ "$SLAVE_TESTS" == "1" ]
    then
	if [ -z  $SLAVE ]
	then 
	    echo "Error: the name of the slave is required"
	    exit 1
	fi
    fi
	
}
#########################################################################


VERSION="1.36-25"

EXPECTED_FAILURES=0
PLATFORM="i386_linux24"
EGEE_VERSION=0
#ALIEN_ORGANISATION="Egg04"

IGNORE=""

GET_ARGUMENTS $*

ALIEN_ROOT=${ALIEN_ROOT:="/opt/alien"}
ALIEN="${ALIEN_ROOT}/bin/alien"
ALIEN_TESTDIR=${ALIEN_TESTDIR:="$ALIEN_ROOT/test"}
export OPENSSL_CONF=$ALIEN_TESTDIR/openssl.cnf


if [ !  -d $ALIEN_ROOT ] 
then
    echo "USING THE DEFAULT INSTALLATION"
    ALIEN_ROOT="/opt/alien";
fi

#export ALIEN_ORGANISATION

if [ "$ALL" == "1" ]
then 
    echo "Doing all the tests $0"
    for TESTS in "-install -root_tests -user_tests -no_break" "-portal_install -portal" "-cvs_install -root_tests -no_break"
    do
	$0 -version $VERSION -platform $PLATFORM $TESTS 
	[ $? -ne 0 ] && echo "error!!" && exit -1
    done
    echo "All the tests suceeded"
    exit;
fi

for FUNCTION in "NEW_MACHINE" "CREATE_CERT" "INSTALL" "CVS_INSTALL" \
    "ROOT_TESTS" "NEW_VO" "USER_TESTS" "PORTAL" "SLAVE_INSTALL" \
    "SLAVE_TESTS" "PACKAGE_TESTS" "GAS_TESTS" "GAPI_TESTS" "JOB_TESTS" "CATALOGUE_TESTS" \
    "PERFORMANCE_TESTS" "BANK_TESTS" "TRANSFERS_TESTS" "JOB_MANUAL_TESTS"
do
    if [ "${!FUNCTION}" == "1" ] ;
    then 
	case `type -t ALIEN_$FUNCTION` in
	    function)
		echo "Starting the tests"
		date
		ALIEN_$FUNCTION
		;;
	    *) 
		LIST="${FUNCTION}_LIST"
		TESTS="${!LIST}"
		echo "Doing the $FUNCTION "
		ALIEN_TESTS
		;;
	esac
    fi	    
done

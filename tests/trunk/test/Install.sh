#!/bin/sh
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
ALIEN_TESTS()
{
    echo "Checking if the certificate is ok"
    env PATH=$ALIEN_ROOT/bin:$PATH LD_LIBRARY_PATH=$ALIEN_ROOT/lib:$LD_LIBRARY_PATH openssl verify -CApath $ALIEN_ROOT/globus/share/certificates -purpose sslclient $HOME/.alien/globus/usercert.pem

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
    CAFILE=$ALIEN_ROOT/globus/share/certificates/`openssl x509 -hash -noout < $HOME/.alien/globus/usercert.pem`.0
    for TEST in $TESTS
    do
	FILE="$DIR/$i.test.$TEST"

	printf "Test %2i %-27s ......................  " $i "$TEST..."

	number=${TEST%%-*}

	if [ "$number" == "$BEGIN" ]
	then
		ACTIVE=1
	fi

	if [ "$IGNORE" != "${IGNORE/$number//}" ] || [ $ACTIVE -ne 1 ]
	then
	    echo ignored
	else
	    rm -rf $DIR/current
	    ln -s $FILE $DIR/current
	    START=`date +"%s"`
	    $ALIEN -x $ALIEN_TESTDIR/${TEST}.t >$FILE 2>&1
	    #cat $FILE
	    DONE=$?
	    END=`date +"%s"`
	    let TIME=$END-$START
	    $ALIEN proxy-info > /dev/null 2>&1  && echo -n "WARNING THERE IS A PROXY"
	    [ -f "$CAFILE"  ] ||  echo -n "WARNING THE CA $CAFILE IS NOT IN X509_CERT_DIR"
	    $ALIEN proxy-destroy > /dev/null 2>&1
	    printf "(%-4s seconds)" $TIME
	    grep "Use of uninitialized value" $FILE  && DONE=22
	    grep "masks earlier declaration in same scope" $FILE  && DONE=22
	    grep "Useless use of " $FILE  && DONE=22
            grep "not ok " $FILE && DONE=22
            
# 	ERROR=`grep -i error $FILE`
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
			cat $FILE
			exit -2
		    fi
		fi
		echo
	    else
		echo "ok"
		rm -f $FILE
	    fi
	fi
	let i++
    done
    allEnd=`date +"%s"`
    nSuccess=`expr $nTests - $FAILEDTESTS`
    SEND_TO_ML "${FUNCTION}_nTests" $nTests \
		"${FUNCTION}_nSuccess" $nSuccess \
		"${FUNCTION}_pSuccess" `expr $nSuccess \* 100 / $nTests` \
		"${FUNCTION}_time" `expr $allEnd - $allStart`
    echo "In total, there are $FAILEDTESTS tests that failed (there were supposed to be $EXPECTED_FAILURES"
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
    export LD_LIBRARY_PATH=$ALIEN_ROOT/lib:$PATH

    USERDIR=$HOME/.alien/globus
    DIR=$HOME/.alien/etc/aliend/ldap/certs/
    KEY=$DIR/host.key.pem
    CERT=$DIR/host.cert.pem
    COMMENTS[0]="Creating the directory for the certificate"
    COMMENTS[1]="Creating the certificate"
    COMMENTS[2]="Changing the privileges"
    COMMENTS[3]="Creating a self signed certificate"
    COMMENTS[4]="Changing the privileges"
    COMMENTS[5]="Making a copy of the certificate"
    COMMENTS[6]="Making a copy of the key"
    COMMENTS[7]="Creating CA file"
    COMMENTS[8]="Using $HOME/.globus"
    COMMENTS[9]="Running alien config"

    COMMANDS[0]="mkdir -p $DIR $USERDIR"
    COMMANDS[1]="openssl genrsa -out $KEY 1024 > /dev/null 2>&1"
    COMMANDS[2]="chmod go-rwx $KEY"
    COMMANDS[3]="openssl req -new -batch -key $KEY -x509 -days 365 -out $CERT"
    COMMANDS[4]="chmod go-rwx $CERT"
    COMMANDS[5]="cp -f $CERT $USERDIR/usercert.pem"
    COMMANDS[6]="cp -f $KEY $USERDIR/userkey.pem"
    COMMANDS[7]="cp $CERT $ALIEN_ROOT/globus/share/certificates/\`openssl x509 -hash -noout < $CERT\`.0"
    COMMANDS[8]="rm -rf $HOME/.globus;ln -s  $USERDIR $HOME/.globus"
    COMMANDS[9]="$ALIEN_ROOT/bin/alien config "

    which openssl
    EXECUTE_SHELL "CREATE_CERT"

    CAFILE=$ALIEN_ROOT/globus/share/certificates/`openssl x509 -hash -noout < $USERDIR/usercert.pem`.0

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
BANK_TESTS_LIST="304-putBankDataLDAP 301-bankAccount 302-transactFunds 303-addFunds "

JOB_TESTS_LIST="$BANK_TESTS_LIST 70-x509 89-jdl 19-ClusterMonitor 21-submit 73-updateCE 22-execute 305-execOrder 62-inputfile 23-resubmit 26-ProcessMonitorOutput 305-execOrder 105-killRunningJob 85-inputdata 94-inputpfn 98-jobexit 64-jobemail 77-rekill 86-split 87-splitFile 88-splitArguments 115-queueList 118-validateJob 119-outputDir 120-production 124-OutputArchive 126-OutputInSeveralSE 133-queueInfo 134-dumplist 135-inputdata2 137-userArchive 140-jobWithMemory 141-executingTwoJobs 152-inputdatacollection 153-splitInputDataCollection 157-zip 159-bigoutput 160-JDLenvironment 161-userGUID"
PACKAGE_TESTS_LIST="75-PackMan 76-jobWithPackage 82-packageDependencies 84-sharedPackage 100-tcshPackage 83-gccPackage 130-localConfig 131-definedPackage"
GAS_TESTS_LIST="69-gContainer 71-GAS 72-UI "
CATALOGUE_TESTS_LIST="63-addEmptyFile 91-expandWildcards 16-add 17-retrieve 74-http 18-metadata 18-metadata 37-find 65-metadata2 15-tree 78-symlink 79-specialChar 95-listDir 93-cpdir 121-cp 101-registerFile 102-secondSE 103-mirror 117-findCaseSensitive 123-VirtualSE 125-mirror 128-modifyMd5 132-listDirectory 136-deleteFile 138-copyFile 139-vi 144-upperCase 146-mv 148-findXML 149-guid2lfn"
TRANSFER_TESTS_LIST="150-ftd 151-submitTransfer"
USER_TESTS_LIST="01-use 116-uninitialized 06-connecting 34-mkdir 07-creating 45-checkOnePerm 52-wrongQuery 51-soapretry 09-ldap 97-pam 08-createKeys 40-forkDatabase 12-certificates 55-httpsConnect 32-rmdir 13-addhost 13-addhost 31-checkdir 46-mysqlConnect 14-se 109-loggerRedirect $CATALOGUE_TESTS_LIST $TRANSFER_TESTS_LIST $JOB_TESTS_LIST 20-xfiles 30-logger 114-silentMode $PACKAGE_TESTS_LIST 68-dbthreads 81-guid 142-mysqlOpenssl 47-killMysql 48-killAliEnProxy "
GAPI_TESTS_LIST="500-apiservice 501-apiservice-connect 502-apiservice-motd 600-aliensh-tokeninit 601-aliensh-tokeninfo 602-aliensh-tokendestroy 600-aliensh-tokeninit 603-aliensh-basics 610-xrootd-se 500-apiservice 610-xrootd-se 620-aliensh-cp-l2se 621-aliensh-cp-se2l 622-aliensh-cp-se2se 623-aliensh-cp-l2l 624-aliensh-rm 630-aliensh-submit 631-aliensh-getzipoutput 632-aliensh-jdl 633-aliensh-trace"


CVS_INSTALL_LIST="10-cvs-co "


NEW_VO_LIST="01-use 02-classads 04-createorgldap 03-createorgdb 61-rotate 05-createorgservices 49-uninitialized"
PERFORMANCE_TESTS_LIST="106-performanceInsert 108-performanceQuery 107-performanceDelete"


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
	    -NO_BREAK|-no_break)
	        NO_BREAK=1
		;;
	    -PORTAL|-portal)
	        PORTAL=1
		;;
	    -INSTALL_PORTAL|-install_portal)
	        INSTALL_PORTAL=1
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
ALIEN_TESTDIR=${ALIEN_TESTDIR:="$PWD/src/test/alien/"}

IGNORE=""

GET_ARGUMENTS $*

ALIEN_ROOT=${ALIEN_ROOT:="/opt/alien.${VERSION}.${PLATFORM}"}
ALIEN="${ALIEN_ROOT}/bin/alien"
export OPENSSL_CONF=$ALIEN_ROOT/openssl.cnf


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
    "PERFORMANCE_TESTS" "BANK_TESTS"
do
    if [ "${!FUNCTION}" == "1" ] ;
    then 
	case `type -t ALIEN_$FUNCTION` in
	    function)
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

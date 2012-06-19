#!/bin/bash
###########################################################################
ALIEN_StartAllServices()
###########################################################################
{
  ( ALIEN_DoService StatusSE )
  test $? -eq 1 && echo Starting SE ..&& ( ALIEN_DoService StartSE )
  ( ALIEN_StatusMonitor )
  test $? -eq 1 && echo Starting Monitor .. && ( ALIEN_StartMonitor )
  ( ALIEN_DoService StatusCE )
  test $? -eq 1 && echo Starting CE .. && ( ALIEN_DoService StartCE )
  ( ALIEN_DoService StatusFTD )
  test $? -eq 1 && echo Starting FTD .. && ( ALIEN_DoService StartFTD )
#  ( ALIEN_DoService StatusTcpRouter )
#  test $? -eq 1 && echo Starting TcpRouter .. && ( ALIEN_DoService StartTcpRouter )
#  ( ALIEN_DoService StatusCLC )
#  test $? -eq 1 && echo Starting CLC .. && ( ALIEN_DoService StartCLC )
  echo Starting MonaLisa && ( ALIEN_StartMonaLisa )
}

###########################################################################
ALIEN_RestartAllServices()
###########################################################################
{
  ( ALIEN_StopAllServices )
  ( ALIEN_StartAllServices )
}

###########################################################################
ALIEN_StopAllServices()
###########################################################################
{
  ( ALIEN_DoService StopSE )
  ( ALIEN_DoService StopMonitor )
  ( ALIEN_DoService StopCE )
  ( ALIEN_DoService StopFTD )
#  ( ALIEN_DoService StopTcpRouter )
#  ( ALIEN_DoService StopCLC )
  ( ALIEN_DoService StopMonaLisa )
}

###########################################################################
ALIEN_StatusMonaLisa()
###########################################################################
{
  statusService MonaLisa MonaLisa
}
###########################################################################
ALIEN_StartMonaLisa()
###########################################################################
{
  if [ -f $ALIEN_ROOT/java/MonaLisa/AliEn/startMonaLisa.pl ]
  then
    $ALIEN_PERL $ALIEN_ROOT/java/MonaLisa/AliEn/startMonaLisa.pl
  fi
  exit 0
}

###########################################################################
ALIEN_StopMonaLisa()
###########################################################################
{
  if [ -f $ALIEN_ROOT/java/MonaLisa/AliEn/stopMonaLisa.pl ]
  then
    $ALIEN_PERL $ALIEN_ROOT/java/MonaLisa/AliEn/stopMonaLisa.pl
  fi
  exit 0
}

###########################################################################
ALIEN_StatusApiService()
###########################################################################
{
  $ALIEN_PERL $ALIEN_ROOT/api/scripts/gapiserver/gapiserver.pl status
  if [ $? = "0" ]; then
      echo "DEAD"
      return 1;
  else 
      return 0;
  fi
}

###########################################################################
ALIEN_StartApiService()
###########################################################################
{
  getLogDir LOGDIR;
  #  $ALIEN_PERL $ALIEN_ROOT/scripts/ApiConfig.pl
  export GSHELL_ROOT=$ALIEN_ROOT/api;
  if [ $? != "0" ]; then
      exit -1;
  fi

 if [ -f $ALIEN_ROOT/api/scripts/gapiserver/gapiserver.pl ]
  then
    $ALIEN_PERL $ALIEN_ROOT/api/scripts/gapiserver/gapiserver.pl start $LOGDIR/ApiService.log
  fi
  exit 0
}

###########################################################################
ALIEN_StopApiService()
###########################################################################
{
  if [ -f $ALIEN_ROOT/api/scripts/gapiserver/gapiserver.pl ]
  then
    $ALIEN_PERL $ALIEN_ROOT/api/scripts/gapiserver/gapiserver.pl stop 
  fi
  exit 0
}


########################################################################### 
FindLocation() 
########################################################################### 
{ 
  STANDARD_DIRS="$ALIEN_ROOT/ $ALIEN_ROOT/api/ $ALIEN_ROOT/globus/ /opt/globus/ /opt/glite/ /opt/glite/externals/ /usr/local/ /usr/bin/ /usr/lib/" 
  if [ "$2" != "" ] 
  then 
     echo $2 
  else 
     for dir in $STANDARD_DIRS 
     do 
       if [ -d $dir ] 
       then 
         if test "`uname -s`" = "Darwin" ; then 
           file=`find -L $dir -path $1 -type f -print -maxdepth 3 2>/dev/null` 
         else 
           file=`find $dir -path $1 -xtype f -print -maxdepth 3 2>/dev/null` 
         fi 
         if [ -f "$file" ] 
         then 
           bindir=`dirname $file` 
           dirname $bindir 
           break 
         fi 
       fi 
     done 
  fi 
} 

      
###########################################################################
ALIEN_HttpdStart()
###########################################################################
{
  serviceName=$1
  portNum=$2
  hostAddress=$3

  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ALIEN_ROOT/api/lib:$ALIEN_ROOT/httpd/lib
  export PERL5LIB=$PERL5LIB:$ALIEN_ROOT/lib/perl5/site_perl/5.10.1:$ALIEN_ROOT/lib/perl5/5.10.1

  ######### 
  # globus 
  ######### 
  set -f 
  GLOBUS_LOCATION=`FindLocation "*/bin/grid-proxy-init" $GLOBUS_LOCATION` 

  if [ ! -f $GLOBUS_LOCATION/bin/grid-proxy-init ] 
  then 
     printf "Error: GLOBUS_LOCATION not set correctly: %s\n" $GLOBUS_LOCATION 
     exit 1 
  fi 

  tmpN=$serviceName
  tmpName=$(echo $tmpN | tr [a-z] [A-Z])
 
  CONF="$ALIEN_HOME/httpd/conf.$portNum/httpd.conf.$hostAddress"
   
  if [ -f $CONF ]
    then
      SETSID=`which setsid 2> /dev/null`
      $SETSID  $ALIEN_ROOT/httpd/bin/httpd -k start -f $CONF 2>&1     
    else
      echo "THE FILE $CONF does not exist"
      exit -2
  fi
}

      
##########################################################################
ALIEN_GetPortNumber()
{
  local  __resultvar=$1
  local tmpN=$2
  local hostAddress=$3
  

  local portNum2=`echo ${hostAddress##*:}`
  
  if [ $tmpN == "Authen" ]
     then
      portNum2=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl AUTH_PORT 2> /dev/null`
  elif [ $tmpN == "IS" ]
     then
      portNum2=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl IS_PORT 2> /dev/null`
  elif [ $tmpN == "Logger" ]
    then
      portNum2=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl LOG_PORT 2> /dev/null`
  elif [ $tmpN == "Monitor" ]
    then
      portNum2=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl CLUSTERMONITOR_PORT 2> /dev/null`
  elif [ $tmpN == "JobAgent" ]
    then
      allPorts=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl PROCESS_PORT_LIST 2> /dev/null`
      for portNum2 in $allPorts ; 
      do 
      	 echo "Checking if $portNum2 is available"
         local lsof="/usr/sbin/lsof"
         [ -f  '/usr/bin/lsof' ] && lsof="/usr/bin/lsof"
      	 $lsof -i:$portNum2 > /dev/null 2>&1
      	 if [ $? -eq "1" ];
      	 then
      	 	echo "Port $portNum2 is free!"
      	 	break
      	 fi      	 
      done
      if [ -z "$portNum2" ];
      then
      	echo "We couldn't find a port for the JobAgent"
      	exit -2
      fi
      export ALIEN_JOBAGENT_PORT=$portNum2
      echo "AND IN THE SERVICES.SH $ALIEN_CM_AS_LDAP_PROXY"
      
  fi

  eval $__resultvar=$portNum2
  
}

###########################################################################
ALIEN_CreateHTTPDConfiguration()
###########################################################################
{
  local portNum=$1
  local hostAddress=$2
  local startupFormat=$3
  local HTTPS=$4
  
  local CONF="$ALIEN_HOME/httpd/conf.$portNum/httpd.conf.$hostAddress"
  
  
  export ALIEN_HOME=$HOME/.alien
  #echo "THE CONFIGURATION WILL BE IN $CONF (with $portNum, $hostAddress, $startupFormat and $HTTPS"
  
  if [ ! -d $ALIEN_HOME/httpd/conf."$portNum" ] || [ ! -f $CONF ] || [ ! -f $ALIEN_HOME/httpd/conf."$portNum"/startup.pl ]
   then
               if [ -f $CONF ]
               then
                        rm -f $CONF
               fi
         mkdir -p ${ALIEN_HOME}/httpd/conf."$portNum"/
         echo "Creating the configuration file $CONF"
         cat  <<EOF > $CONF
PidFile $LOGDIR/${startupFormat}.$portNum.pid
Listen $portNum


ErrorLog $LOGDIR/${startupFormat}.$portNum.log
ServerRoot $ALIEN_ROOT/httpd/

LoadModule perl_module     modules/mod_perl.so

# Possible values include: debug, info, notice, warn, error, crit,
# alert, emerg.
#
LogLevel warn

LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
LogFormat "%h %l %u %t \"%r\" %>s %b" common
CustomLog $LOGDIR/${startupFormat}_access.$portNum.log common

#Prefork module settings
<IfModule prefork.c>
StartServers 2
MinSpareServers 2
MaxSpareServers 15
MaxClients 50
MaxRequestsPerChild 50
</IfModule>

PerlSwitches -I$ALIEN_ROOT/lib/perl5 -I$ALIEN_ROOT/lib/perl5/site_perl
PerlModule Apache2::compat

PerlPassEnv  HOME
PerlPassEnv  ALIEN_ROOT
PerlPassEnv  ALIEN_DOMAIN
PerlPassEnv  ALIEN_USER
PerlPassEnv  ALIEN_HOME
PerlPassEnv  ALIEN_ORGANISATION
PerlPassEnv  ALIEN_LDAP_DN
PerlPassEnv  GLOBUS_LOCATION
PerlPassEnv  X509_USER_PROXY
PerlPassEnv  X509_CERT_DIR
PerlPassEnv  ALIEN_JOBAGENT_PORT
PerlPassEnv  ALIEN_CM_AS_LDAP_PROXY
PerlConfigRequire $ALIEN_HOME/httpd/conf.$portNum/startup.pl

EOF
        [ "$SSL" == "1" ] && cat <<EOF >> $CONF 

SSLengine on
SSLSessionCache dbm:   $ALIEN_ROOT/httpd/logs/ssl_gcache_data
SSLCertificateFile    $ALIEN_HOME/globus/hostcert.pem
SSLCertificateKeyFile  $ALIEN_HOME/globus/hostkey.pem
SSLVerifyClient require
SSLVerifyDepth  10
SSLOptions +StdEnvVars
SSLCACertificatePath $ALIEN_ROOT/globus/share/certificates/

EOF
	     echo "<Location /> " >> $CONF
         [ "$SSL" == "1" ] && echo "SSLRequireSSL" >> $CONF
    
     
         cat >> $CONF <<EOF
     
     SetHandler perl-script
     PerlResponseHandler JSON::RPC::Server::Apache2
     PerlSetVar dispatch "AliEn::Service::$startupFormat"
     PerlSetVar return_die_message 0
     PerlSetVar options "compress_threshold => 10000"
     PerlOptions +SetupEnv
     Allow from all
</Location>
EOF
         cat > $ALIEN_HOME/httpd/conf."$portNum"/startup.pl <<EOF
use strict;

use AliEn::Logger;

my @services=qw( $startupFormat ) ;

my \$userID = getpwuid(\$<);


\$ENV{ALIEN_HOME} = ( \$ENV{ALIEN_HOME} || "/home/\$userID/.alien" );
\$ENV{ALIEN_ROOT} = ( \$ENV{ALIEN_ROOT} || "/home/\$userID/alien") ;
\$ENV{ALIEN_USER} = ( \$ENV{ALIEN_USER} || "\$userID" );
\$ENV{ALIEN_ORGANISATION} = (\$ENV{ALIEN_ORGANISATION} ||  "ALICE" );
\$ENV{GLOBUS_LOCATION} = ( \$ENV{GLOBUS_LOCATION} || "\$ENV{ALIEN_ROOT}/globus" ) ;
\$ENV{X509_USER_PROXY} = ( \$ENV{X509_USER_PROXY} || "/tmp/x509up_u\$<" );
\$ENV{X509_CERT_DIR} = ( \$ENV{X509_CERT_DIR} || "\$ENV{GLOBUS_LOCATION}/share/certificates" );
EOF
    for var in SEALED_ENVELOPE_REMOTE_PUBLIC_KEY SEALED_ENVELOPE_REMOTE_PRIVATE_KEY SEALED_ENVELOPE_LOCAL_PUBLIC_KEY \
               SEALED_ENVELOPE_LOCAL_PRIVATE_KEY ALIEN_DATABASE_PASSWORD ;
    do
    	if [ "${!var}" != "" ] ;
    	then 
    		echo "\$ENV{$var} = '${!var}' ;" >> $ALIEN_HOME/httpd/conf."$portNum"/startup.pl
    	fi	
    done
     

         cat >> $ALIEN_HOME/httpd/conf."$portNum"/startup.pl <<EOF


my \$l=AliEn::Logger->new();
\$l->redirect("$LOGDIR/$startupFormat.$portNum.log");

foreach my \$s (@services) {
  print "Checking \$s\n";
  my \$name="AliEn::Service::\$s";
  eval {
    eval "require \$name" or die("Error requiring the module: \$@");
    my \$serv=\$name->new() ;
    \$serv or exit(-2);
  };
  if (\$@) {
    print "NOPE!!\n \$@\n";

    exit(-2);
  }
  \$l->info("HTTPD", "Starting \$s on $hostAddress:$portNum");

}

print "ok\n";        
EOF
		fi

   if [ ! -f $CONF ] || [ ! -f $ALIEN_HOME/httpd/conf."$portNum"/startup.pl ]
     then
      echo "$ALIEN_HOME/httpd/conf.$portNum/ don't have httpd.conf and startup.pl"
      exit 1
   fi            
  
}


###########################################################################
ALIEN_StartHttp ( )
###########################################################################
{
  local serviceName=$1
  local configName=$2
  local startupFormat=$3
  

 
  local hostName=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl $configName 2> /dev/null `
  local SECURE=0
  if [[ $hostName == https* ]]
    then        
    hostAddress=${hostName#https://}
	SECURE=1     
  elif [ -n "$hostName" ] ;  
    then 
	hostAddress=${hostName#http://}
  else    
    echo "RUNNING ON THE LOCAL MACHINE"
    hostAddress=`hostname -f`
  fi
    

  ALIEN_GetPortNumber portNum $serviceName $hostAddress    
  ALIEN_CreateHTTPDConfiguration $portNum $hostAddress $startupFormat $SECURE
      
  ALIEN_HttpdStart $serviceName $portNum $hostAddress
   
}

###########################################################################
getLogDir()
###########################################################################
{
  DIR=$1
  if [ "$DIR" = "LOGDIR" ]
    then
       DIR=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl LOG_DIR`
       LOGDIR=$DIR
    else
	   LOGDIR="$ALIEN_HOME/var/log/AliEn/$ALIEN_ORGANISATION"
	   F="$ALIEN_HOME/etc/aliend/$ALIEN_ORGANISATION/startup.conf"
       [ -f "$F" ] && source $F	   
  fi
}
###########################################################################
startService()
###########################################################################
{
  local packageName=$1
  local service=$2
  local configName=$3
  shift 3

  #export ALIEN_CM_AS_LDAP_PROXY=0;

  if [ "$1" = "-silent" ]
  then
    SILENT=1
    shift 1
  fi

  if [ "$1" = "alienServiceNOKILL" ]
  then
    [ -n "$SILENT" ] || echo "Not killing other $service"
    NOKILL=1
    shift 1
  fi

  if [ "$1" = "alienServiceLOG" ]
  then
    LOGDIR=$2
    shift 2
  fi



  if [ ! -n "$ALIEN_START" ]
  then
    [ -n "$SILENT" ] || echo "Starting with generic Service.pl"
    ALIEN_START="$ALIEN_ROOT/scripts/Service.pl $packageName"
  fi

  [ -d $LOGDIR ] || mkdir -p $LOGDIR
  chmod 0777 $LOGDIR

  [ -n "$SILENT" ] || echo "Starting the $service"

  [ -f  $LOGDIR/$service.pid ] && OLDPID=`cat $LOGDIR/$service.pid`

  if [ ! -n "$NOKILL" ]
  then
    stopService $packageName $service $configName 1
    if [ $? -eq 0 ]
    then
      [ -n "$SILENT" ] || echo "Killing old $service (pid $OLDPID)"
    fi
  fi
  [ -f $LOGDIR/$service.log ] && mv $LOGDIR/$service.log $LOGDIR/$service.log.back

  rm -rf $LOGDIR/.$service.log.LCK*

 
  
  if [ "$HTTPService" = 1 ] 
  then  
    ALIEN_StartHttp $service $configName $packageName
  else
    SETSID=`which setsid 2> /dev/null`
    $SETSID  $ALIEN_PERL $ALIEN_DEBUG $ALIEN_START $* -logfile $LOGDIR/$packageName.log  &
    error=$?
    MASTERPID=$!
    echo  $! > $LOGDIR/$packageName.pid
  fi

  [ -n "$SILENT" ] || echo "$service started with $error (pid  $!)"

  [ -n "$SILENT" ] || echo "Log file: $LOGDIR/$packageName.$portNum.log "

  return 0
}





###########################################################################
statusService()
###########################################################################
{
  local packageName=$1
  local service=$2
  local configName=$3
  shift 3
  
  

  export ALIEN_CM_AS_LDAP_PROXY=0;
 
  PINGOUTPUT=`${ALIEN_ROOT}/scripts/alien -x $ALIEN_ROOT/scripts/pingService.pl $packageName $LOGDIR 2>&1`
  if [ "$?" -eq "0" ] ; then
    return 0
  fi
  echo $PINGOUTPUT
  
  if [ $service = 'Authen' ] ;
  then 
      [ -f "/home/alienmaster/checkMysql.sh" ] && \
      echo "THERE WAS A PROBLEM TALKING TO Authen!! Checking the database" && \   
         /home/alienmaster/checkMysql.sh
  fi
  
  return 1
}

###########################################################################
stopService()
###########################################################################
{
  local packageName=$1
  local service=$2
  local configName=$3
  shift 3


  QUIET=$1
  

  # kill the service
  
  for KILLFILE in `ls $LOGDIR/$packageName.*pid 2>/dev/null`; do
  	 [ -f  $KILLFILE ] && OLDPIDS=`echo -n "$OLDPIDS ";cat $KILLFILE` 	 
  done 

  ERROR=1;

  TOKILL=""
  for param in $OLDPIDS
  do
    if [ "$param" ]
    then
      PIDS=`ps -A -o "pid ppid pgid" |grep "$param"| awk '{print $1}'`
      TOKILL="$TOKILL $PIDS";
     fi
  done
   
  if [ ! -z "$TOKILL" ];
  then
     ERROR=0
     kill $TOKILL >& /dev/null
     kill -9 $TOKILL >& /dev/null
     sleep 5
     kill $TOKILL >& /dev/null
     kill -9 $TOKILL >& /dev/null
     if [ "$HTTPService" = "1" ]
      then
  	    [ "$QUIET" = 1 ] || echo "And killing the semaphores"
  	    ipcs -s | grep ${USER:0:8} |  perl -e 'while (<STDIN>) {@a=split(/\s+/); print `ipcrm sem $a[1]`}' > /dev/null 2>&1
     fi
     
  fi
  
  ps -u $USER -fww |grep " $service "  |grep -v grep |grep -v $$ >/dev/null 2>&1

  if [ "$?" = "0" ];
  then
    [ "$QUIET" = 1 ] || echo "It looks like there was something still running..."
    kill -9 `ps -u $USER -fww |grep  " $service " |grep -v grep |grep -v $$ |awk '{print \$2}'`
  fi

  for KILLFILE in `ls $LOGDIR/$packageName.*pid 2>/dev/null`; do
  	 [ -f  $KILLFILE ] && rm  -f $KILLFILE 	 
  done   
  
  if [ "$ERROR" = "1" ]
   then
    [ "$QUIET" = 1 ] || echo "Service  $service was dead!!"
  fi

  return $ERROR
}


###########################################################################
ALIEN_DoService ()
###########################################################################
{
    cmd=$1
    

    operation=""
     
    [ "$cmd" != "${cmd##Start}" ] && operation="startService" && pattern="Start"
    
    [ "$cmd" != "${cmd##Status}" ] && operation="statusService" && pattern="Status"

    [ "$cmd" != "${cmd##Stop}" ] &&	operation="stopService" && pattern="Stop"

    [ -z "$operation" ]  && return
    shift 1

    
    local service=${cmd##$pattern}
    extra_args=""
    export ALIEN_PROCESSNAME=$service
    arguments=${service#?*_}
    arguments=${arguments//_/ }
    service=${service%%_?*}
    
    local configName="${service}_HOST"
	local packageName="$service"
	
    LOGDIR="VARDIR"
    
    HTTPService=1
    case $service in 
	  CE)
	    file="CE"
	    if [ "$2" = '-queue' ] 
	    then 
		file="CE_$3"
	    fi

	    if [ "$arguments" != "${arguments#-queue }" ]
	    then 
		file="CE_${arguments#-queue }"
	    fi
	    
	    ALIEN_START="$ALIEN_ROOT/scripts/CE.pl"
	    LOGDIR="LOGDIR"
	    HTTPService=0	    
	    ;;
      CMreport)
        ALIEN_START="$ALIEN_ROOT/scripts/CMreport.pl"
  	    LOGDIR="LOGDIR"
	    HTTPService=0	    
            ;;
	  FTD)
	    HTTPService=0	    
	    ;;
	  IS)
	    ;;
	  JobBroker|Broker)
	    service="JobBroker"
	    configName="JOB_BROKER_ADDRESS"
	    packageName="Broker::Job"
	    ;;
	  JobManager|Server)
	    service="JobManager"
	    configName="JOB_MANAGER_ADDRESS"
	    packageName="Manager::Job"
	    ;;
	  JobInfoManager)
	    configName="JOBINFO_MANAGER_ADDRESS"
	    packageName="Manager::JobInfo"
	    ;;
	  Authen)
	    configName="AUTH_HOST"
	    ;;
	  TransferOptimizer)
	    packageName="Optimizer::Transfer"
	    HTTPService=0
	    ;;
	  TransferBroker)
        configName="TRANSFER_BROKER_ADDRESS"
        packageName="Broker::Transfer"
	    ;;
	  TransferManager)
	    configName="TRANSFER_MANAGER_ADDRESS"
	    packageName="Manager::Transfer"	    
	    ;;
	  SEManager)
	    configName="SEMASTER_MANAGER_ADDRESS"
	    packageName="Manager::SEMaster"	    	    
	    ;;
	  MessagesMaster)	
	    configName="MESSAGESMASTER_ADDRESS"
	    packageName="MessagesMaster"
	    ;;
	  JobOptimizer)
	    packageName="Optimizer::Job"
	    HTTPService=0
	    ;;
	  CatalogueOptimizer)
	    packageName='Optimizer::Catalogue'
	    HTTPService=0
	    ;;
	  MonaLisa)
        operation="ALIEN_${cmd}"
        LOGDIR="LOGDIR"
	    ;;
	  Monitor)
	   	LOGDIR="LOGDIR"
	   	packageName="ClusterMonitor"	   
	    ;;
	  *)
	    echo "I don't know the service $service"
	    exit -2
	    ;;
	  
	esac
	
    getLogDir $LOGDIR
        
	args="$packageName $service $configName $* $arguments $extra_args"
	
	[ $pattern  != "Start" ] && args="$args  || echo -n '$service is down!!'"
	
	$operation $args
	
	error=$?	
	exit $error
   

}

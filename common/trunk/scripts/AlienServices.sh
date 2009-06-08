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
  statusService LOGDIR MonaLisa
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
  $ALIEN_PERL $ALIEN_ROOT/scripts/ApiConfig.pl
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
ALIEN_StartMonitor()
###########################################################################
{
  mod="normal"

  arg=""


  for param in $*
  do
    case $param in
      --batch|-batch)
              ALIEN_MODE=BATCH
	      export ALIEN_MODE
	      mod="batch";;
      *)
	 arg="$arg $param"
    esac
  done

  export ALIEN_PROCESSNAME=Monitor

  startService LOGDIR ClusterMonitor  "cluster monitor" NO_PASSWORD $arg

  if [ "$ALIEN_MODE" = "BATCH"  ]
    then
      [ -n "$SILENT" ] || echo WAITING
      wait
  fi

  exit  
}
###########################################################################
ALIEN_StatusMonitor()
###########################################################################
{
  statusService LOGDIR ClusterMonitor || exit 1;
  exit 0;
}
###########################################################################
ALIEN_StopMonitor()
###########################################################################
{
  stopService LOGDIR ClusterMonitor || echo -n "Monitor was down"
  exit 
}

###########################################################################
ALIEN_Starthttpd()
###########################################################################
{

  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ALIEN_ROOT/api/lib
  for file in `find  $HOME/.alien/httpd -name httpd.conf` ; 
  do 
     echo "CHECKING $file"
    $ALIEN_ROOT/httpd/bin/httpd -k start -f $file  # >/dev/null 2>&1
  done
  exit $?
}

###########################################################################
ALIEN_Statushttpd()
###########################################################################
{
  ps -ef |grep httpd |grep -v grep |grep -v Statushttpd> /dev/null 2>&1
  exit $?
}

###########################################################################
ALIEN_Stophttpd()
###########################################################################
{
  $ALIEN_ROOT/httpd/bin/httpd -k stop >/dev/null 2>&1
  exit $?
}

###########################################################################
startService()
###########################################################################
{
  ORIGDIR=$1
  FILE=$2
  SERVICE=$3
  REQ_PASSWD=$4

  shift 4

  export ALIEN_CM_AS_LDAP_PROXY=0;
    
  getLogDir $ORIGDIR
    
  if [ "$1" = "-silent" ]
  then
    SILENT=1
    shift 1
  fi

  if [ "$1" = "alienServiceNOKILL" ]
  then 
    [ -n "$SILENT" ] || echo "Not killing other $SERVICE"
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
    ALIEN_START="$ALIEN_ROOT/scripts/Service.pl $FILE" 
  fi 

  [ -d $LOGDIR ] || mkdir -p $LOGDIR
  chmod 0777 $LOGDIR

  [ -n "$SILENT" ] || echo "Starting the $SERVICE"

  if [ "$REQ_PASSWD" = "PASSWORD" ]
  then
    if [ "$1" = "-passwd" ]
    then
      PASSWD=`cat $2`
      shift 2
    else
      echo -n "Enter admin password: "
      stty -echo
      read PASSWD
      echo
      stty echo
    fi
  fi
  
  [ -f  $LOGDIR/$FILE.pid ] && OLDPID=`cat $LOGDIR/$FILE.pid`

  if [ ! -n "$NOKILL" ]
  then
    stopService $ORIGDIR $FILE 1
    if [ $? -eq 0 ] 
    then 
      [ -n "$SILENT" ] || echo "Killing old $SERVICE (pid $OLDPID)"
    fi
  fi
  [ -f $LOGDIR/$FILE.log ] && mv $LOGDIR/$FILE.log $LOGDIR/$FILE.log.back

  rm -rf $LOGDIR/.$FILE.log.LCK*
 
  if [ "$AFS" = "true" ]
  then
    if [ `echo $ALIEN_HOME | grep -c '^/afs'` -eq 1  ]
    then
      ALIEN_PERL="ALIEN_AFS $ALIEN_PERL"
      printf "AFS Password: "
      AFS_PASSWORD=`ReadPassword`
     fi 
   fi
   SETSID=`which setsid 2> /dev/null`

   $SETSID  $ALIEN_PERL $ALIEN_DEBUG $ALIEN_START $* -logfile $LOGDIR/$FILE.log <<EOF &
$PASSWD
EOF
  error=$?
  MASTERPID=$!
  echo  $! > $LOGDIR/$FILE.pid
  
  [ -n "$SILENT" ] || echo "$SERVICE started with $error (pid  $!)"

  [ -n "$SILENT" ] || echo "Log file: $LOGDIR/$FILE.log "

  return 0
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
       DIR="/var/log/AliEn/$ALIEN_ORGANISATION"
        LOGDIR="/var/log/AliEn/$ALIEN_ORGANISATION"
	[ "$USER" = "root" ] || LOGDIR=$ALIEN_HOME$LOGDIR
  fi
}
###########################################################################
statusService()
###########################################################################
{
  DIR=$1
  NAME=$2

  export ALIEN_CM_AS_LDAP_PROXY=0;

  getLogDir $DIR

  PINGOUTPUT=`${ALIEN_ROOT}/scripts/alien -x $ALIEN_ROOT/scripts/pingService.pl $2 $LOGDIR 2>&1`
  if [ "$?" -eq "0" ] ; then
    return 0
  fi
  echo $PINGOUTPUT
  return 1
}
###########################################################################
stopService()
###########################################################################
{
  DIR=$1
  NAME=$2
  QUIET=$3

  export ALIEN_CM_AS_LDAP_PROXY=0;

  getLogDir $DIR

# kill the service

  KILLFILE="$LOGDIR/$NAME.pid"

  [ -f  $KILLFILE ] && OLDPIDS=`cat $KILLFILE`

  ERROR=1;


  TOKILL=""

  for param in $OLDPIDS
  do
    if [ "$param" ]
    then
      PIDS=`ps -A -o "pid ppid pgid" |grep " $param\$"|awk '{print $1}'`
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
  fi

  ps -u $USER -fww |grep " $NAME "  |grep -v grep |grep -v $$ >/dev/null 2>&1

  if [ "$?" = "0" ];
  then
    [ "$QUIET" = 1 ] || echo "It looks like there was something still running..."
    kill -9 `ps -u $USER -fww |grep  " $NAME " |grep -v grep |grep -v $$ |awk '{print \$2}'`
  fi

  [ -f  $KILLFILE ] &&  rm  -f $KILLFILE
  
  if [ "$ERROR" = "1" ]
   then
    [ "$QUIET" = 1 ] || echo "Service was dead!!"
  fi
  return $ERROR
}

###########################################################################
ALIEN_DoService ()
###########################################################################
{
    cmd=$1

    operation=""
    
    [ $cmd != ${cmd##Start} ] && operation="startService" && pattern="Start"
    
    [ $cmd != ${cmd##Status} ] && operation="statusService" && pattern="Status"

    [ $cmd != ${cmd##Stop} ] &&	operation="stopService" && pattern="Stop"


    [ -z "$operation" ]  && return

    service=${cmd##$pattern}
    extra_args=""
    export ALIEN_PROCESSNAME=$service
    arguments=${service#?*_}
    arguments=${arguments//_/ }
    service=${service%%_?*}

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
	    args="LOGDIR $file \"CE\"  NO_PASSWORD"
	    ALIEN_START="$ALIEN_ROOT/scripts/CE.pl"
	    ;;
	SE)
	    args='LOGDIR SE  "Storage_element" NO_PASSWORD'
	    ;;
	FTD)
	    args='LOGDIR FTD  "File_Transfer_Daemon" NO_PASSWORD'
	    ;;
	IS)
	    args='VARDIR IS  "Information_service" NO_PASSWORD  '
	    ;;
	JobBroker|Broker)
            export ALIEN_PROCESSNAME=JobBroker
	    args='VARDIR Broker::Job "Resource_Broker" NO_PASSWORD'
	    ;;
	JobManager|Server)
            export ALIEN_PROCESSNAME=JobManager
	    args='VARDIR  Manager::Job "Queue_Server" NO_PASSWORD'
	    ;;
	JobInfoManager)
            export ALIEN_PROCESSNAME=JobInfoManager
	    args='VARDIR  Manager::JobInfo "JobInfo_Manager" NO_PASSWORD'
	    ;;
	Proxy)
	    PROXY_PORT=`${ALIEN_ROOT}/scripts/alien -org $ALIEN_ORGANISATION -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl PROXY_PORT`
			if [ -z "$PROXY_PORT" ] 
			then
				 [ -n "$SILENT" ] || echo "Error getting the port of the proxy. Is ldap running?"
				exit 255
			fi
	    export ALIEN_PROCESSNAME=ProxyServer
	    export SSL_VERSION=2
	    ALIEN_START="$ALIEN_ROOT/scripts/ProxyServer.pl"
#	    export SSL_CIPHER_LIST=EDH-RSA-DES-CBC3-SHA:EDH-DSS-DES-CBC3-SHA:DES-CBC3-SHA:DES-CBC3-MD5:DHE-DSS-RC4-SHA:IDEA-CBC-SHA:RC4-SHA:RC4-MD5:IDEA-CBC-MD5:RC2-CBC-MD5:RC4-MD5:RC4-64-MD5:EXP1024-DHE-DSS-RC4-SHA:EXP1024-RC4-SHA:EXP1024-DHE-DSS-DES-CBC-SHA:EXP1024-DES-CBC-SHA:EXP1024-RC2-CBC-MD5:EXP1024-RC4-MD5:EDH-RSA-DES-CBC-SHA:EDH-DSS-DES-CBC-SHA:DES-CBC-SHA:DES-CBC-MD5:EXP-EDH-RSA-DES-CBC-SHA:EXP-EDH-DSS-DES-CBC-SHA:EXP-DES-CBC-SHA:EXP-RC2-CBC-MD5:EXP-RC4-MD5:EXP-RC2-CBC-MD5:EXP-RC4-MD5
	    args="VARDIR  ProxyServer Proxy_Server PASSWORD"
	    extra_args="--localport $PROXY_PORT"
	    ;;
        Logger)
	    args='VARDIR Logger  "Logger" NO_PASSWORD'
	    ;;
        SOAPProxy)
	    args='SOAPProxy SOAPProxy "SOAPProxy" NO_PASSWORD'
	    ;;
	Authen)
	    args='VARDIR  Authen "Authentication" PASSWORD'
#	    ALIEN_START="$ALIEN_ROOT/scripts/Authen.pl"
	    export SEALED_ENVELOPE_REMOTE_PUBLIC_KEY=$ALIEN_HOME/authen/rpub.pem
	    export SEALED_ENVELOPE_REMOTE_PRIVATE_KEY=$ALIEN_HOME/authen/rpriv.pem
	    export SEALED_ENVELOPE_LOCAL_PUBLIC_KEY=$ALIEN_HOME/authen/lpub.pem
	    export SEALED_ENVELOPE_LOCAL_PRIVATE_KEY=$ALIEN_HOME/authen/lpriv.pem
	    ;;
	  TransferOptimizer)
	    args='VARDIR Optimizer::Transfer Transfer_Optimizer NO_PASSWORD'
	    ;;
	  TransferBroker)
	    args="VARDIR Broker::Transfer Transfer_Broker  NO_PASSWORD"
	    ;;
	  TransferManager)
	    args='VARDIR Manager::Transfer "Transfer_Manager"  NO_PASSWORD'
	    ;;
	  SEManager)
	    args='VARDIR Manager::SEMaster "SE_Manager" NO_PASSWORD'
	    ;;
	  PackManMaster)
	    args='VARDIR PackManMaster "PackManMaster"  NO_PASSWORD'
	    ;;
	  MessagesMaster)
	    args='VARDIR MessagesMaster "MessagesMaster"  NO_PASSWORD'
	    ;;
	  Interface)
	    args='VARDIR Interface "Interface"  NO_PASSWORD'
	    ;;
	  Secure)
	    args='VARDIR Secure "Secure"  NO_PASSWORD'
	    ;;
	  JobOptimizer)
	    args='VARDIR Optimizer::Job Job_Optimizer NO_PASSWORD'
	    ;;
	  SecureFactory)
	    args='VARDIR SecureFactory SecureFactory NO_PASSWORD'
	    ;;
	  Secure)
	    args='LOGDIR Secure Secure NO_PASSWORD'
	    ;;
	  SOAPProxy)
	    args='VARDIR SOAPProxy "SOAPProxy"  NO_PASSWORD'
	    ;;
	  CatalogueOptimizer)
	    args='VARDIR Optimizer::Catalogue Catalogue_Optimizer NO_PASSWORD'
	    ;;
	  gContainer)
	    exec $ALIEN_PERL $ALIEN_DEBUG ${ALIEN_ROOT}/scripts/gContainer.pl $pattern $*
	    ;;
	  Container)
	    ALIEN_START="${ALIEN_ROOT}/AliEn/Portal/scripts/Container.pl"
	    args="VARDIR  Container Container NO_PASSWORD"
	    ;;
 	  PackMan)
	    args='LOGDIR PackMan PackMan NO_PASSWORD'
	    ;;
	  FC)
	    args="VARDIR FC FC NO_PASSWORD"
	    ;;
	  Config)
	    args="VARDIR Config Config NO_PASSWORD"
	    ;;
	esac

    if [ -n "$args" ]
    then 
	shift 1
	args="$args $* $arguments $extra_args"
	[ $pattern  != "Start" ] \
		&& args="$args  || echo -n '$service is down!!'"
	$operation $args
	exit $?
    fi

}

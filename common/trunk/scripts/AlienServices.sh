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
  ALIEN_CheckSoapType ClusterMonitor

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
ALIEN_OneHttpdService()
###########################################################################
{
  serviceName=$1


  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ALIEN_ROOT/api/lib:$ALIEN_ROOT/httpd/lib
  export PERL5LIB=$PERL5LIB:$ALIEN_ROOT/lib/perl5/site_perl/5.8.8:$ALIEN_ROOT/lib/perl5/5.8.8

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


  logDir=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl LOG_DIR`
  # echo $logDir
  tmpN=$serviceName
  tmpName=$(echo $tmpN | tr [a-z] [A-Z])
  portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl "$tmpName"_PORT 2> /dev/null `


      if [ $tmpN == "Authen" ]
           then
              portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl AUTH_PORT 2> /dev/null `
        
       fi

       if [ $tmpN == "JobBroker" ] || [ $tmpN == "Broker" ]
           then
              portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl BROKER_PORT 2> /dev/null `
       fi
       
       
 if [ -f $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf ]
  then
        file=$ALIEN_HOME/httpd/conf."$portNum"/httpd.conf
        $ALIEN_ROOT/httpd/bin/httpd -k start -f $file  # >/dev/null 2>&1

  fi
   sleep 5
  if [ -f $ALIEN_ROOT/httpd/logs/httpd"$serviceName".pid ]
  then
       if [ $tmpN == "Authen" ]
       	   then
       	   ps -ef | grep httpd | grep $portNum | awk '{if ($3==1) print $2}' > $ALIEN_ROOT/httpd/logs/httpd"$serviceName".pid
       fi
       
       if [ $tmpN == "ClusterMonitor" ]
       	   then
       	   ps -ef | grep httpd | grep $portNum | awk '{if ($3==1) print $2}' > $ALIEN_ROOT/httpd/logs/httpd"$serviceName".pid
       fi
       
       
       cp $ALIEN_ROOT/httpd/logs/httpd"$serviceName".pid $logDir/"$serviceName".pid
   
       if [ $tmpN == "JobBroker" ] || [ $tmpN == "Broker" ]
           then
           ps -ef | grep httpd | grep $portNum | awk '{if ($3==1) print $2}' > $ALIEN_ROOT/httpd/logs/httpdBroker.pid
           cp $ALIEN_ROOT/httpd/logs/httpdBroker.pid $ALIEN_ROOT/httpd/logs/httpdJobBroker.pid
           cp $ALIEN_ROOT/httpd/logs/httpdBroker.pid $logDir/"Broker::Job".pid
           cp $ALIEN_ROOT/httpd/logs/httpdBroker.pid $logDir/JobBroker.pid
       fi
   
  fi

  exit $?
}




###########################################################################
ALIEN_HttpdConfig()
###########################################################################
{

        export ALIEN_HOME=$HOME/.alien
        tmpN=$1
        httpdFormat="AliEn::Service::""$tmpN"
        startupFormat=$tmpN

        tmpName=$(echo $tmpN | tr [a-z] [A-Z])
         portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl "$tmpName"_PORT 2> /dev/null `
       # echo $portNum
       
       if [ $tmpN == "Authen" ]
           then
              httpdFormat="AliEn::Service::Authen"
              portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl AUTH_PORT 2> /dev/null `
        
       fi

       if [ $tmpN == "JobBroker" ] || [ $tmpN == "Broker" ]
           then
              httpdFormat="AliEn::Service::Broker::Job"
              portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl BROKER_PORT 2> /dev/null `
              
       fi

        if [ ! -f $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf ] || [ ! -f $ALIEN_HOME/httpd/conf."$portNum"/startup.pl ]
        then
               if [ -d $ALIEN_HOME/httpd/conf."$portNum" ]
               then
                        rm -rf $ALIEN_RHOME/httpd/conf."$portNum"
               fi
         mkdir -p ${ALIEN_HOME}/httpd/conf."$portNum"/
         cp $ALIEN_ROOT/httpd/conf/httpd.conf $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf
         cp $ALIEN_ROOT/httpd/conf/startup.pl $ALIEN_HOME/httpd/conf."$portNum"/startup.pl
        fi


         sed -e "s#PerlConfigRequire .*#PerlConfigRequire $ALIEN_HOME/httpd/conf."$portNum"/startup.pl#" $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf > /tmp/httpd.$$
         cp /tmp/httpd.$$ $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf

         sed -e "s#Listen .*#Listen $portNum#" $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf > /tmp/httpd.$$
         cp /tmp/httpd.$$ $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf
         
         sed -e "s#^PidFile .*#PidFile logs/httpd"$tmpN".pid#" $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf > /tmp/httpd.$$
         cp /tmp/httpd.$$ $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf
         
         sed -e "s#PerlSetVar dispatch_to.*#PerlSetVar dispatch_to \"$ALIEN_ROOT/lib/perl5/site_perl/5.10.1 $httpdFormat \"#" $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf> /tmp/httpd.$$
         cp /tmp/httpd.$$ $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf

         sed -e "s#my @services=.*#my @services=qw( $startupFormat ) ;#" $ALIEN_HOME/httpd/conf."$portNum"/startup.pl > /tmp/startup.$$
         cp /tmp/startup.$$ $ALIEN_HOME/httpd/conf."$portNum"/startup.pl
         
         if [ $tmpN == "JobBroker" ] || [ $tmpN == "Broker" ]
         then
         	sed -e "s#my @services=.*#my @services=qw( Broker::Job ) ;#" $ALIEN_HOME/httpd/conf."$portNum"/startup.pl > /tmp/startup.$$
         	cp /tmp/startup.$$ $ALIEN_HOME/httpd/conf."$portNum"/startup.pl
         fi


          rm /tmp/httpd.$$
          rm /tmp/startup.$$

        
        if [ -f $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf ] && [ -f $ALIEN_HOME/httpd/conf."$portNum"/startup.pl ]
        then
              ALIEN_OneHttpdService $startupFormat

        else
              echo "$ALIEN_HOME/httpd/conf.$portNum/ don't have httpd.conf and startup.pl"
              exit 1
         fi

}
      
      
###########################################################################
ALIEN_HttpdStart()
###########################################################################
{
  serviceName=$1
  portNum=$2


  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ALIEN_ROOT/api/lib:$ALIEN_ROOT/httpd/lib
  export PERL5LIB=$PERL5LIB:$ALIEN_ROOT/lib/perl5/site_perl/5.8.8:$ALIEN_ROOT/lib/perl5/5.8.8

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


  logDir=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl LOG_DIR`
  # echo $logDir
  tmpN=$serviceName
  tmpName=$(echo $tmpN | tr [a-z] [A-Z])
 
       
 if [ -f $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf ]
  then
        file=$ALIEN_HOME/httpd/conf."$portNum"/httpd.conf
        $ALIEN_ROOT/httpd/bin/httpd -k start -f $file  # >/dev/null 2>&1

  fi
   sleep 5
  if [ -f $ALIEN_ROOT/httpd/logs/httpd"$serviceName".pid ]
  then
       ps -ef | grep httpd | grep $portNum | awk '{if ($3==1) print $2}' > $ALIEN_ROOT/httpd/logs/httpd"$serviceName".pid
     
      
       
       cp $ALIEN_ROOT/httpd/logs/httpd"$serviceName".pid $logDir/"$serviceName".pid
   
    #    if [ $tmpN == "JobBroker" ] || [ $tmpN == "Broker" ]
    #        then
    #       ps -ef | grep httpd | grep $portNum | awk '{if ($3==1) print $2}' > $ALIEN_ROOT/httpd/logs/httpdBroker.pid
    #       cp $ALIEN_ROOT/httpd/logs/httpdBroker.pid $ALIEN_ROOT/httpd/logs/httpdJobBroker.pid
    #        cp $ALIEN_ROOT/httpd/logs/httpdBroker.pid $logDir/"Broker::Job".pid
    #        cp $ALIEN_ROOT/httpd/logs/httpdBroker.pid $logDir/JobBroker.pid
    #    fi
   
  fi

  exit $?
}

      

###########################################################################
ALIEN_HttpsConfig()
###########################################################################
{
export ALIEN_HOME=$HOME/.alien
        tmpN=$1
        hostAddress=$2
        httpdFormat=$3
        startupFormat=`echo $httpdFormat | sed 's/AliEn::Service:://'`
        
        
        portNum=`echo ${hostAddress##*:}`
        if [ $tmpN == "Authen" ]
           then
             portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl AUTH_PORT 2> /dev/null`
        elif [ $tmpN == "IS" ]
           then
             portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl IS_PORT 2> /dev/null`
        elif [ $tmpN == "Logger" ]
           then
             portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl LOG_PORT 2> /dev/null`
        elif [ $tmpN == "PackMan" ]
           then
             portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl PACKMAN_PORT 2> /dev/null`
        elif [ $tmpN == "Monitor" ]
           then
             portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl CLUSTERMONITOR_PORT 2> /dev/null`
        fi
       # echo $portNum
        
        
         if [ ! -f $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf ] || [ ! -f $ALIEN_HOME/httpd/conf."$portNum"/startup.pl ]
        then
               if [ -d $ALIEN_HOME/httpd/conf."$portNum" ]
               then
                        rm -rf $ALIEN_RHOME/httpd/conf."$portNum"
               fi
         mkdir -p ${ALIEN_HOME}/httpd/conf."$portNum"/
         cp $ALIEN_ROOT/httpd/conf/httpd.conf $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf
         cp $ALIEN_ROOT/httpd/conf/startup.pl $ALIEN_HOME/httpd/conf."$portNum"/startup.pl
        fi


         sed -e "s#PerlConfigRequire .*#PerlConfigRequire $ALIEN_HOME/httpd/conf."$portNum"/startup.pl#" $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf > /tmp/httpd.$$
         cp /tmp/httpd.$$ $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf

         sed -e "s#Listen .*#Listen $portNum#" $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf > /tmp/httpd.$$
         cp /tmp/httpd.$$ $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf
         
         sed -e "s#^PidFile .*#PidFile logs/httpd"$tmpN".pid#" $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf > /tmp/httpd.$$
         cp /tmp/httpd.$$ $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf
         
         sed -e "s#PerlSetVar dispatch_to.*#PerlSetVar dispatch_to \"$ALIEN_ROOT/lib/perl5/site_perl/5.10.1 $httpdFormat \"#" $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf> /tmp/httpd.$$
         cp /tmp/httpd.$$ $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf

         sed -e "s#my @services=.*#my @services=qw( $startupFormat ) ;#" $ALIEN_HOME/httpd/conf."$portNum"/startup.pl > /tmp/startup.$$
         cp /tmp/startup.$$ $ALIEN_HOME/httpd/conf."$portNum"/startup.pl
         
         if [ $tmpN == "JobBroker" ] || [ $tmpN == "Broker" ]
         then
         	sed -e "s#my @services=.*#my @services=qw( Broker::Job ) ;#" $ALIEN_HOME/httpd/conf."$portNum"/startup.pl > /tmp/startup.$$
         	cp /tmp/startup.$$ $ALIEN_HOME/httpd/conf."$portNum"/startup.pl
         fi


          rm /tmp/httpd.$$
          rm /tmp/startup.$$

        
        if [ -f $ALIEN_HOME/httpd/conf."$portNum"/httpd.conf ] && [ -f $ALIEN_HOME/httpd/conf."$portNum"/startup.pl ]
        then
              ALIEN_HttpdStart $tmpN $portNum

        else
              echo "$ALIEN_HOME/httpd/conf.$portNum/ don't have httpd.conf and startup.pl"
              exit 1
         fi     

        

}                                                         

###########################################################################
ALIEN_IsHttps ( )
###########################################################################
{

  serviceName=$1
  configName=$(echo $ServiceName | tr [a-z] [A-Z])
  packageName="AliEn::Service::""$serviceName"

   [ $serviceName == "Proxy" ] && configName="PROXY_ADDRESS" && packageName="AliEn::Services::ProxyServer"
   [ $serviceName == "IS" ] && configName="IS_HOST"
   [ $serviceName == "Authen" ] && configName="AUTH_HOST" && packageName="AliEn::Service::Authen"
   
   [ $serviceName == "Server" ] && configName="JOB_MANAGER_ADDRESS" && packageName="AliEn::Service::Manager::Job"
   
   [ $serviceName == "Logger" ] && configName="LOG_HOST"
   
   [ $serviceName == "Broker" ] && configName="JOB_BROKER_ADDRESS" && packageName="AliEn::Service::Broker::Job"
   
   [ $serviceName == "TransferManager" ] && configName="TRANSFER_MANAGER_ADDRESS" && packageName="AliEn::Service::Manager::Transfer"
   [ $serviceName == "TransferBroker" ] && configName="TRANSFER_BROKER_ADDRESS" && packageName="AliEn::Service::Broker::Transfer"
   [ $serviceName == "TransferOptimizer" ] && configName="TRANSFER_OPTIMIZER_ADDRESS" && packageName="AliEn::Service::Optimizer::Transfer"
   
   [ $serviceName == "JobOptimizer" ] && configName="JOB_OPTIMIZER_ADDRESS" && packageName="AliEn::Service::Optimizer::Job"
   
   [ $serviceName == "CatalogueOptimizer" ] && configName="CATALOGUE_OPTIMIZER_ADDRESS" && packageName="AliEn::Service::Optimizer::Catalogue"
   
   [ $serviceName == "PackManMaster" ] && configName="PACKMANMASTER_ADDRESS" 
   [ $serviceName == "MessagesMaster" ] && configName="MESSAGESMASTER_ADDRESS"
   
   
   [ $serviceName == "SEManager" ] && configName="SEMASTER_MANAGER_ADDRESS" && packageName="AliEn::Service::Manager::SEMaster"
   
   [ $serviceName == "JobInfoManager" ] && configName="JOBINFO_MANAGER_ADDRESS" && packageName="AliEn::Service::Manager::JobInfo"
   
   [ $serviceName == "PackMan" ] && configName="PACKMAN_HOST" && packageName="AliEn::Service::PackMan"
    
   [ $serviceName == "Monitor" ] && configName="CLUSTERMONITOR_ADDRESS" && packageName="AliEn::Service::ClusterMonitor"
   
  # [ $serviceName == "CE" ] && configName="CE_HOST" && packageName="AliEn::Service::CE" 
   
    hostName=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl $configName 2> /dev/null `
    # echo $serviceName
    echo $hostName
   	if [[ $hostName == https* ]]
          	  then
          	 		 echo "$serviceName  wants to be started as a httpd "
          	 	 	 ALIEN_HttpsConfig $serviceName $hostName $packageName
             		 exit 0
       fi
          	  
    return 0 	  
    
}




###########################################################################
ALIEN_CheckSoapType ( )
###########################################################################
{
   ServiceName=$1
   tmpName=$(echo $ServiceName | tr [a-z] [A-Z])
   HttpdType=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl ${tmpName}_SOAPTYPE 2> /dev/null `
   portNum=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl ${tmpName}_PORT 2> /dev/null `
   # echo "$tmpName $ServiceName is $HttpdType $portNum";
     if [ "$HttpdType" != "httpd" ]
     then
          if [ $ServiceName == "Authen" ]
          then
              authHost=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl AUTH_HOST 2> /dev/null `
          	  if [[ $authHost == https* ]]
          	  then
          	 		 echo "Authen service wants to be started as a httpd "
          	 	 	 ALIEN_HttpdConfig $ServiceName
             		 exit 0
          	  fi
          fi
          
            
           
           if [ $ServiceName == "JobBroker" ] || [ $ServiceName == "Broker" ]
           then
              brokerHost=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl JOB_BROKER_ADDRESS 2> /dev/null `
          	  if [[ $brokerHost == https* ]]
          	  then
          	 		 echo "JobBroker service wants to be started as a httpd "
          	 	 	 ALIEN_HttpdConfig $ServiceName
             		 exit 0
          	  fi
          fi
          
      #    echo "the soapType of the $ServiceName is not httpd "
          if [ -d $ALIEN_HOME/httpd/conf."$portNum" ]
          then
                rm -rf $ALIEN_HOME/httpd/conf."$portNum"
          fi
      
           return 0
     else
     #   echo "the soapType of the $ServiceName is httpd "
         ALIEN_HttpdConfig $ServiceName
         exit 0
    fi

 }




###########################################################################
ALIEN_Starthttpd()
###########################################################################
{
  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ALIEN_ROOT/api/lib:$ALIEN_ROOT/httpd/lib
  export PERL5LIB=$PERL5LIB:$ALIEN_ROOT/lib/perl5/site_perl/5.8.8:$ALIEN_ROOT/lib/perl5/5.8.8
  export GLOBUS_LOCATION=$ALIEN_ROOT/globus
  logDir=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl LOG_DIR`
 # echo $logDir

  for file in `find  $HOME/.alien/httpd -name httpd.conf` ;
  do
     echo "CHECKING $file"
    $ALIEN_ROOT/httpd/bin/httpd -k start -f $file  # >/dev/null 2>&1

   stringNew=`cat $file | grep "PerlSetVar dispatch_to" | grep -v "#" `
   stringOld=`echo $stringNew | sed 's/ /\n/g' | grep "AliEn::Service" | sed 's/AliEn::Service:://' `

  sleep 3
   cp $ALIEN_ROOT/httpd/logs/httpd"$stringOld".pid $logDir/"$stringOld".pid
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
  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ALIEN_ROOT/api/lib:$ALIEN_ROOT/httpd/lib
  export PERL5LIB=$PERL5LIB:$ALIEN_ROOT/lib/perl5/site_perl/5.8.8:$ALIEN_ROOT/lib/perl5/5.8.8
  logDir=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl LOG_DIR`
  # echo $logDir
  for file in `find  $HOME/.alien/httpd -name httpd.conf` ;
  do
     echo "CHECKING $file"
     $ALIEN_ROOT/httpd/bin/httpd -k stop -f $file  # >/dev/null 2>&1

  stringNew=`cat $file | grep "PerlSetVar dispatch_to" | grep -v "#" `
  stringOld=`echo $stringNew | sed 's/ /\n/g' | grep "AliEn::Service" | sed 's/AliEn::Service:://' `

  echo $stringOld
  rm  $logDir/"$stringOld".pid
  done
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
	   F="$ALIEN_HOME/etc/aliend/$ALIEN_ORGANISATION/startup.conf"
	   [ -f "$F" ] && source $F
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
  
  
  
   [ $NAME == "Proxy" ] && configName="PROXY_ADDRESS" 
   [ $NAME == "IS" ] && configName="IS_HOST"
   [ $NAME == "Authen" ] && configName="AUTH_HOST" 
   
   [ $NAME == "Manager::Job" ]  && configName="JOB_MANAGER_ADDRESS" 
   
   [ $NAME == "Logger" ] && configName="LOG_HOST"
   
   [ $NAME == "Broker::Job" ] && configName="JOB_BROKER_ADDRESS" 
   
   [ $NAME == "Manager::Transfer" ] && configName="TRANSFER_MANAGER_ADDRESS" 
   [ $NAME == "Broker::Transfer" ] && configName="TRANSFER_BROKER_ADDRESS" 
   [ $NAME == "Optimizer::Transfer" ] && configName="TRANSFER_OPTIMIZER_ADDRESS" 
   
   [ $NAME == "Optimizer::Job" ] && configName="JOB_OPTIMIZER_ADDRESS" 
   
   [ $NAME == "Optimizer::Catalogue" ] && configName="CATALOGUE_OPTIMIZER_ADDRESS" 
   
   [ $NAME == "PackManMaster" ] && configName="PACKMANMASTER_ADDRESS" 
   [ $NAME == "MessagesMaster" ] && configName="MESSAGESMASTER_ADDRESS"
   
   
   [ $NAME == "Manager::SEMaster" ] && configName="SEMASTER_MANAGER_ADDRESS" 
   
   [ $NAME == "Manager::JobInfo" ] && configName="JOBINFO_MANAGER_ADDRESS" 
   
   [ $NAME == "PackMan" ] && configName="PACKMAN_HOST" 
    
   [ $NAME == "ClusterMonitor" ] && configName="CLUSTERMONITOR_ADDRESS" 
  
 
    hostAddress=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl $configName 2> /dev/null ` 
  		
  		if [[ $hostAddress == https* ]]
  		then 
  	        tmpDir=`${ALIEN_ROOT}/scripts/alien -x ${ALIEN_ROOT}/scripts/GetConfigVar.pl LOG_DIR`
  	 
  	        [ $NAME == "Manager::Job" ] && NAME="Server"
  	        [ $NAME == "Broker::Job" ] && NAME="Broker"
  	        [ $NAME == "Manager::Transfer" ] && NAME="TransferManager"
  	        [ $NAME == "Broker::Transfer" ] && NAME="TransferBroker"
  	        [ $NAME == "Optimizer::Transfer" ] && NAME="TransferOptimizer"
  	        [ $NAME == "Optimizer::Job" ] && NAME="JobOptimizer"
  	        [ $NAME == "Optimizer::Catalogue" ] && NAME="CatalogueOptimizer"
  	        [ $NAME == "Manager::SEMaster" ] && NAME="SEManager"
  	        [ $NAME == "Manager::JobInfo" ] && NAME="JobInfoManager"
  	        
  	      	KILLFILE="$tmpDir/$NAME.pid"	
  	     # 	echo $KILLFILE      	
  	      		
  	      		      	
  		fi
   	

  [ -f  $KILLFILE ] && OLDPIDS=`cat $KILLFILE`

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
 
  # echo $OLDPIDS
# echo $TOKILL
  
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
    [ "$QUIET" = 1 ] || echo "$NAME Service was dead!!"
  fi
  
   	
  	if [[ $hostAddress == https* ]]
  		then 
  		   
  	        rm -f $ALIEN_ROOT/httpd/logs/httpd"$NAME".pid 
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
    


    if [ $pattern = "Start" ]
    then
        #  ALIEN_CheckSoapType $service

         ALIEN_IsHttps $service 
    fi

 
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
	    args='VARDIR  Authen "Authentication" NO_PASSWORD'
#	    ALIEN_START="$ALIEN_ROOT/scripts/Authen.pl"
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
	
	error=$?
	
	if [ $operation$service = 'statusServiceAuthen' ];
	then 
	  if [ $error != 0 ];
	  then
	    echo "THERE WAS A PROBLEM TALKING TO Authen!! Checking the database"
	    [ -f "/home/alienmaster/checkMysql.sh" ] &&  /home/alienmaster/checkMysql.sh
	  fi
	fi
	exit $error
    fi

}

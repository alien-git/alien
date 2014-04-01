#!/bin/sh
#############################################################################
# alien - a front-end script to access Alice Environment
#############################################################################
#
# modification history
#
# SYNOPSIS
# alien [flags] <command [options]>
#
# DESCRIPTION
#
# AUTHOR:
#
# CREATION DATE:
# 13-Aug-2001
#C<
###########################################################################
[ -f $ALIEN_ROOT/scripts/VERSION ] && . $ALIEN_ROOT/scripts/VERSION
[ -z "$VERSION" ] || export  ALIEN_VERSION=$VERSION


###########################################################################
export ALIEN_PROMPT=${ALIEN_PROMPT:="alien"}
export ALIEN_CA=${ALIEN_CA:="/var/log/AliEn/CA"}
###########################################################################
ALIEN_Usage()
###########################################################################
{
   printf "Usage: %5s [-help]                  \n" $ALIEN_PROMPT
   printf "             [-user <account>][-host <host>]  \n"
   printf "             [-exec <command>]                \n"
   printf "             [-x <script>]                    \n"
   printf "             [-domain <domain>]               \n"
   printf "             [-organisation <organisation>]   \n"
   printf "             [-printenv]                      \n" 
   printf "             [-platform]                      \n" 
   printf "             [login]                          \n" 
   printf "             [proof]                          \n"
   printf "             [virtual]                        \n"
   printf "             [proxy-*]                        \n"
   printf "             [cert-request]                   \n" 
   printf "             [register-cert]                  \n"
   printf "             [man]                            \n"
   printf "             [configure] [options]            \n"
   exit
}


###########################################################################
ALIEN_Printenv()
###########################################################################
{
   printenv | grep -e ALIEN_ -e X509 -e GLOBUS -e PERL -e SSL_CIPHER_LIST
   echo "PERL5LIB=$ALIEN_ROOT/lib/perl5/site_perl:$ALIEN_ROOT/lib/perl5"
   exit
}

###########################################################################
ALIEN_Login()
###########################################################################
{
 exec $ALIEN_PERL  $ALIEN_DEBUG $ALIEN_ROOT/scripts/Prompt.pl "::LCM::Computer" $*
}

###########################################################################
ALIEN_Proof()
###########################################################################
{
 exec $ALIEN_PERL  $ALIEN_DEBUG $ALIEN_ROOT/scripts/Prompt.pl "::LCM::Proof" $*
}
###########################################################################
ALIEN_Browser()
###########################################################################
{
  exec $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/Prompt.pl "::LCM" $*
}
###########################################################################
ALIEN_Virtual()
###########################################################################
{
  exec $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/Prompt.pl  $*
}

###########################################################################
ALIEN_RunAgent()
###########################################################################
{
  export ALIEN_PROCESSNAME=JOBAGENT

  #exec $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/Service.pl JobAgent $*
  unset SEALED_ENVELOPE_REMOTE_PUBLIC_KEY SEALED_ENVELOPE_REMOTE_PRIVATE_KEY SEALED_ENVELOPE_LOCAL_PUBLIC_KEY \
               SEALED_ENVELOPE_LOCAL_PRIVATE_KEY ALIEN_DATABASE_PASSWORD 
  
  HTTPService=1
  getLogDir LOGDIR
  startService JobAgent JobAgent JobAgent alienServiceNOKILL
  HTTPService=0
  sleep 2
  PID=`cat $LOGDIR/JobAgent.$portNum.pid `
  echo "And now, the agent itself (PID OF HTTPD $PID) or  $! from $LOGDIR/JobAgent.$portNum.pid  "
  startService JobAgent JobAgent JobAgent alienServiceNOKILL  -pid $PID 
  exit
}

###########################################################################
ALIEN_Platform()
###########################################################################
{
  echo `uname -s`-`uname -m`-`uname -r | cut -d "." -f 1-2`-gnu-`gcc -dumpversion`;  
}


###########################################################################
ALIEN_Package_list()
###########################################################################
{
  cd ~/.alien/packages;
  ls *
  cd -;
  exit;
}

###########################################################################
ALIEN_Package_download()
###########################################################################
{
  # arg 1: Software Name f.e. 'ROOT'/'AliROOT'
  # arg 2: Software Version  f.e. '3.01.01'
  # arg 3: URL form where to download from the zipped tar file  
  printf "! Please Wait ! \n";
  if [ -z $1 ] 
  then 
    echo "Usage: Package_Download <NAME> <VERSION> <URL>";
    exit -1;
  fi

  if [ -z $2 ] 
  then 
    echo "Usage: Package_Download <NAME> <VERSION> <URL>";
    exit -1;
  fi

  if [ -z $3 ]
  then 
    echo "Usage: Package_Download <NAME> <VERSION> <URL>";
    exit -1;
  fi

  olddir=$PWD;
  mkdir -p ~/.alien/packages;
  test -d ~/.alien/packages || (echo "Error: can't create ~/.alien/packages" && exit -1;)
  cd ~/.alien/packages ; 
  mkdir -p $1; 
  test -d $1 || (echo "Error: can't create ~/.alien/packages/$1" && exit -1;)
  cd $1; 
  mkdir $2; 
  test -d $2 || (echo "Error: can't create ~/.alien/packages/$1/$2" && exit -1;)
  cd $2;

  test -f `basename $3` && cp `basename $3` `basename $3`.orig; wget -N -K $3; diff -q `basename $3`  `basename $3`.orig || ( tar xzf `basename $3`;)
  cd $olddir;
  exit;
}

###########################################################################
ALIEN_Monitor()
###########################################################################
{
  if [ -f $ALIEN_ROOT/java/MonaLisa/AliEn/stopMonaLisa.pl ]
  then 
    $ALIEN_PERL $ALIEN_ROOT/java/MonaLisa/AliEn/stopMonaLisa.pl
  fi
  if [ -f $ALIEN_ROOT/java/MonaLisa/AliEn/startMonaLisa.pl ]
  then  
    $ALIEN_PERL $ALIEN_ROOT/java/MonaLisa/AliEn/startMonaLisa.pl
  fi
  exit 0
}

###########################################################################
ReadPassword()
###########################################################################
{
  stty -echo
  read p
  stty echo
  echo $p
}
###########################################################################
SetupCertEnvVars()
###########################################################################
{
  dirs="$HOME/.globus $ALIEN_HOME/globus"
  if [ "$ALIEN_ORGANISATION" != "" ]
  then
    org=`echo $ALIEN_ORGANISATION | awk '{print tolower($0)}'`
    dirs=$dirs" "$ALIEN_HOME/identities.$org
  fi
  for dir in $dirs
  do
    if [ -f $dir/usercert.pem  -a -f $dir/userkey.pem ]
    then
       export X509_USER_CERT=$dir/usercert.pem
       export X509_USER_KEY=$dir/userkey.pem
    fi
  done
}
###########################################################################
ALIEN_Grid()
###########################################################################
{
  SetupCertEnvVars
#  if [ -z $GLOBUS_LOCATION ]
#  then
#    printf "Error: GLOBUS_LOCATION not set.\n"
#    printf "Please (re)run 'alien config' command.\n\n"
#    exit 1
#  fi
  bits=" " 
  if [ "$1" = "proxy-init" ] ;
  then
	bits=" -bits 1024"
    echo "Checking the time difference"
    if [ -f /usr/sbin/ntpdate ] ;
    then
      ALIEN_NTP_HOST=${ALIEN_NTP_HOST:=pool.ntp.org}
      DIFF=` /usr/sbin/ntpdate   -q $ALIEN_NTP_HOST |grep 'offset'  |head -n 1 |awk -F offset '{print $2}' |awk -F , '{print $1}' |awk '{print $1$2}' |awk -F . '{print $1}'`
      S=`expr $DIFF \< 300`
      if [ "$S" = 0 ] ;
      then
         echo "Your clock doesn't seem to be synchronized. Please run 'ntpdate $ALIEN_NTP_HOST'"
         exit -2
      fi
    fi
  fi


  if [ -x $GLOBUS_LOCATION/bin/grid-$1 ]
  then 
    exec $GLOBUS_LOCATION/bin/grid-$* $bits
  else
    printf "%s: No such file or directory.\n" $1
    exit 1
  fi
}
###########################################################################
ALIEN_RegisterCert()
###########################################################################
{
  exec $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/updateCert.pl $*
}
###########################################################################
ALIEN_CertRequest()
###########################################################################
{
  exec $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/requestCert.pl $*
}
###########################################################################
ALIEN_Man()
###########################################################################
{
  export MANPATH=$ALIEN_ROOT/man
  exec man $*
}
###########################################################################
ALIEN()
###########################################################################
{
  cmd=$1; 

  ALIEN_DoService $*

  case `type -t ALIEN_$cmd` in
     function)
           shift 1
           ALIEN_$cmd $*
            ;;
          *) 
	   ALIEN_Prompt $ALIEN_UI $*
            ;;
  esac

  exit
}
###########################################################################
ALIEN_FS()
###########################################################################
{
  case $1 in
     mount)
           shift 1
           dir=$1; dir=${dir:="$HOME/alienfs"};
           [ ! -d $dir ] && mkdir -p $dir 
           shift 1
           fs=$1;  fs=${fs:="alienfs://$ALIEN_USER"};
           shift 1
           $ALIEN_ROOT/bin/lufsmount $fs@ $dir
           exit
           ;;
     umount)
           shift 1
           dir=$1; dir=${dir:="$HOME/alienfs"};
           shift 1
           $ALIEN_ROOT/bin/lufsumount $dir
           exit
           ;;
          *)
           break
           ;;
  esac
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
ALIEN_Config()
###########################################################################
{
  ALIEN_PATH="$ALIEN_ROOT/api/bin"
  ALIEN_LD_LIBRARY_PATH=$ALIEN_ROOT/lib:$ALIEN_ROOT/lib/mysql:$ALIEN_ROOT/api/lib

  if test "`uname -p`" = "x86_64" -o "`uname -p`" = 'ia64' ; then ALIEN_LD_LIBRARY_PATH="$ALIEN_LD_LIBRARY_PATH:$ALIEN_ROOT/lib64" ; fi

  ALIEN_MYPROXY_SERVER=""
  ALIEN_MYPROXY_DOMAIN=""
  ALIEN_LDAP_DN=""
  GLOBUS_LOCATION=""
  GAPI_LOCATION=""
  MYPROXY_LOCATION=""
  ALIEN_VO=""
  ALIEN_PROMPT=""
  ALIEN_ORGANISATION=""
  ALIEN_USER=""

  if test "`uname -s`" = "Darwin" ; then DY_EXT=dylib ; else DY_EXT=so ; fi

  config_scope="user"

  while [ $# -gt 0 ]
  do
    case $1 in
        user)
            config_scope="user"
            shift 1
           ;;
        installation|common)
            config_scope="installation"
            shift 1
           ;;
        --globus-location|-globus-location)
            shift 1
            GLOBUS_LOCATION=$1; shift 1
           ;;
        --myproxy-location|-myproxy-location)
            shift 1
            MYPROXY_LOCATION=$1; shift 1
            ;;
        --myproxy-server|-myproxy-server)
            shift 1
            ALIEN_MYPROXY_SERVER=$1; shift 1
            ;;
        --myproxy-domain|-myproxy-domain)
            shift 1
            ALIEN_MYPROXY_DOMAIN=$1; shift 1
            ;;
        --ldap-dn|-ldap-dn)
            shift 1
            ALIEN_LDAP_DN=$1; shift 1
            ;;
        --VO|-VO)
            shift 1
            ALIEN_DEFAULT_VO=$1; shift 1
            ;;
        --user|-user)
            shift 1
            ALIEN_DEFAULT_USER=$1; shift 1
            ;;
        --prompt|-prompt)
            shift 1
            ALIEN_DEFAULT_PROMPT=$1; shift 1
            ;;

        --help|-help)
            printf "Usage: %5s configure [user|common]\n" $ALIEN_PROMPT
            printf "             --globus-location     [path]  \n"
            printf "             --myproxy-location    [path]  \n"
            printf "             --myproxy-server      [host]  \n"
            printf "             --myproxy-domain      [dn]    \n"
            printf "             --ldap-dn             [url]   \n"
            printf "             --VO                  [string]\n"
            printf "             --user                [string]\n"
            printf "             --prompt              [string]\n"
            exit
           ;;
        *)
           shift 1
           ;;
    esac
  done


  #########
  # globus
  #########
  set -f
  GLOBUS_LOCATION=`FindLocation "*/bin/grid-proxy-init" $GLOBUS_LOCATION`

  if [ -f $GLOBUS_LOCATION/bin/grid-proxy-init ]
  then
    ALIEN_PATH=$GLOBUS_LOCATION/bin:$ALIEN_PATH
    ALIEN_LD_LIBRARY_PATH=$GLOBUS_LOCATION/lib:$ALIEN_LD_LIBRARY_PATH
      echo "X509_CERT_DIR=$GLOBUS_LOCATION/share/certificates"
  else
     printf "Error: GLOBUS_LOCATION not set correctly: %s\n" $GLOBUS_LOCATION
     exit 1
  fi

  #########
  # GAPI
  #########
  set -f
  GAPI_LOCATION=`FindLocation "*/bin/alien-token-init" $GAPI_LOCATION`

  if [ -f $GAPI_LOCATION/bin/alien-token-init ]
  then
    ALIEN_PATH=$GAPI_LOCATION/bin:$ALIEN_PATH
    ALIEN_LD_LIBRARY_PATH=$GAPI_LOCATION/lib:$ALIEN_LD_LIBRARY_PATH
  else
     printf "Error: GAPI_LOCATION not set correctly: %s\n" $GAPI_LOCATION
     exit 1
  fi

  #########
  # myproxy
  #########

  MYPROXY_LOCATION=`FindLocation "*/bin/myproxy-init" $MYPROXY_LOCATION`

  if [ -x $MYPROXY_LOCATION/bin/myproxy-init ]
  then
    ALIEN_PATH=$MYPROXY_LOCATION/bin:$ALIEN_PATH
    ALIEN_LD_LIBRARY_PATH=$MYPROXY_LOCATION/lib:$ALIEN_LD_LIBRARY_PATH
  else
     printf "Error: MYPROXY_LOCATION not set correctly: %s\n" $MYPROXY_LOCATION
     exit 1
  fi

  case $config_scope in
        user)
            outfile=$ALIEN_HOME/Environment
           ;;
        installation)
            outfile=$ALIEN_ROOT/.Environment
           ;;
        *)
           printf "Error: unknown parameter : %s\n" $config_scope
           exit 1 
           ;;
  esac

  if [ -f $outfile ] 
  then
    `mv -f $outfile $outfile.old`
  fi


  ALIEN_PATH=`echo $ALIEN_PATH |awk  '{gsub(":", "\n" ); print $0 '} |sort -u | awk '{printf "%s:",$1}' | sed 's/:$//'`
  ALIEN_LD_LIBRARY_PATH=`echo $ALIEN_LD_LIBRARY_PATH |awk  '{gsub(":", "\n" ); print $0 '} |sort -u | awk '{printf "%s:",$1}' | sed 's/:$//'`

  echo "ALIEN_PATH=$ALIEN_PATH" >> $outfile
  echo "ALIEN_LD_LIBRARY_PATH=$ALIEN_LD_LIBRARY_PATH" >> $outfile

  if [ ! -z "$ALIEN_MYPROXY_SERVER" ]; then
      echo "ALIEN_MYPROXY_SERVER=$ALIEN_MYPROXY_SERVER" >> $outfile
  fi
  if [ ! -z "$ALIEN_MYPROXY_DOMAIN" ]; then
      echo "ALIEN_MYPROXY_DOMAIN=$ALIEN_MYPROXY_DOMAIN" >> $outfile
  fi
  if [ ! -z "$ALIEN_LDAP_DN" ]; then
      echo "ALIEN_LDAP_DN=$ALIEN_LDAP_DN" >> $outfile
  fi
  if [ ! -z "$GLOBUS_LOCATION" ]; then 
      echo "GLOBUS_LOCATION=$GLOBUS_LOCATION" >> $outfile
      echo "X509_CERT_DIR=$GLOBUS_LOCATION/share/certificates" >> $outfile
  fi
  if [ ! -z "$GAPI_LOCATION" ]; then 
      echo "GAPI_LOCATION=$GAPI_LOCATION" >> $outfile
  fi
  if [ ! -z "$MYPROXY_LOCATION" ]; then 
      echo "MYPROXY_LOCATION=$MYPROXY_LOCATION" >> $outfile
  fi
  if [ ! -z "$ALIEN_PROMPT" ]; then 
      echo "ALIEN_PROMPT=$ALIEN_DEFAULT_PROMPT" >> $outfile
  fi
  if [ ! -z "$ALIEN_DEFAULT_VO" ]; then
      echo "ALIEN_ORGANISATION=$ALIEN_DEFAULT_VO" >> $outfile
  fi
  if [ ! -z "$ALIEN_DEFAULT_USER" ]; then 
      echo "ALIEN_USER=$ALIEN_DEFAULT_USER" >> $outfile
  fi
  
  cat $outfile
  
  exit
}

###########################################################################
ALIEN_AFS()
###########################################################################
{
if [ "$AFS_PASSWORD" = "" ] 
then
  printf "AFS Password: "
  AFS_PASSWORD=`ReadPassword`
fi

pagsh << ==EOF==

  Cleanup() 
  {
    echo "Killing $PIDLIST"
    kill $PIDLIST
    exit
  }

  Rauth() 
  {
    while :
    do
      sleep 3600
      printf "Trying to extend AFS token lifetime..." 
      echo $AFS_PASSWORD | klog -pipe -lifetime 24:00:00 || exit 1
      printf "OK\n"
    done 
  }

  echo $AFS_PASSWORD | klog -pipe -lifetime 24:00:00 || exit 1

  $*

  export PIDLIST="$! $$"

  trap Cleanup  1 2 3 15
  
  Rauth &

==EOF==
}

###########################################################################
ALIEN_Prompt()
###########################################################################
{
    exec $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/Prompt.pl $*
}

###########################################################################
ALIEN_GetArg()
###########################################################################
{
  ALIEN_UI="::LCM"
  AFS="false"
  export ALIEN_USER=${ALIEN_USER:=$USER}
  export ALIEN_PROCESSNAME="Prompt"
  ALIEN_ARG=""
  while [ $# -gt 0 ]
  do
    case $1 in
        -trace|--trace)
            shift 1
            set -vx
            ;;
        -version|--version|-v)
            shift 1 
            if [ "$1" = "" ]
            then
#	      alien -exec version
#	      unset ALIEN_CM_AS_LDAP_PROXY
#	      alien -no_catalog virtual -exec version
#              printf "%s\n" $VERSION
	      
	      V=`cat $ALIEN_ROOT/share/alien/ALIEN_VERSION |awk '{print $2$4}' |awk -F , '{print $1"."$2}'`
	      D=`date +"%b %d %H:%M:%S"`
	      echo "$D  info   Version: $V"
              exit
            fi
            ;;
        -echo|--echo)
            shift 1
            ECHO="echo "
            ;;
        -help|--help)
            shift 1
            ALIEN_Usage
            ;;
        -printenv|--printenv)
            shift 1
            ALIEN_Printenv
            ;;
        -afs|--afs)
            shift 1
            AFS="true"
            ;;
        -exec|--exec)
            shift 1
	    cmd=$1
	    shift 1
	    ALIEN_Prompt $ALIEN_UI --silent $ALIEN_ARG--exec $cmd -- $*
            ;;
        -platform|--platform)
            shift 1
	    uname -srm
            exit
            ;;
        -x)
            shift 1
	    if [ -f  $1 ] 
	    then 
	 	exec $ALIEN_PERL $ALIEN_DEBUG $*
	    else 
		echo "Error: File $1 does not exist !!"
		exit -1
	    fi
            exit
            ;;
        -org*|--org*)
            shift 1
            export ALIEN_ORGANISATION=$1
            shift 1
            ;;
	-u|-user|--user)
            shift 1
            export ALIEN_USER=$1
            shift 1
           ;;
        -domain|--domain)
            shift 1
            export ALIEN_DOMAIN=$1
            shift 1
            ;;
        login)
            shift 1
	    export ALIEN_PROCESSNAME="Prompt::Login"
	    ALIEN_UI="::LCM::Computer"
            ;;
	proof)
	    shift 1
	    export ALIEN_PROCESSNAME="Prompt::Proof"
	    ALIEN_UI="::LCM::Computer::Proof"
	    ;;
	virtual)
	    shift 1
	    ALIEN_UI="virtual"
	    ;;
	cert-request)
	    shift 1
            ALIEN_CertRequest $*
	    ;;
	host-cert-request)
	    shift 1
	    exec $ALIEN_PERL $ALIEN_ROOT/scripts/requestCertificate.pl host
	    ;;
        proxy-*|cert-*)
            ALIEN_Grid $*
            ;;
        man)
            shift 1
            ALIEN_Man $*
            ;;
        fs)
            shift 1
            export PATH=$ALIEN_ROOT/bin:$PATH
            export LD_LIBRARY_PATH=$ALIEN_ROOT/lib/api:$LD_LIBRARY_PATH
            export PERL5LIB=$ALIEN_ROOT/lib/perl5/site_perl:$ALIEN_ROOT/lib/perl5
            ALIEN_FS $*
            ;;
        register-cert)
            shift 1
            ALIEN_RegisterCert $*
            ;;
        config|configure)
            shift 1
            ALIEN_Config $*
            ;;
        *)
	    ALIEN_ARG="$ALIEN_ARG$1 "
	    shift 1
    esac
  done
    
  ALIEN $ALIEN_ARG$* 
}


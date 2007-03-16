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
   printf "             [gas]                            \n" 
   printf "             [proof]                          \n"
   printf "             [virtual]                        \n"
   printf "             [xfiles]                         \n" 
   printf "             [create-keys]                    \n"
   printf "             [update-token]                   \n" 
   printf "             [myproxy-*]                      \n"
   printf "             [proxy-*]                        \n"
   printf "             [cert-request]                   \n" 
   printf "             [register-cert]                  \n" 
   printf "             [job-submit]                     \n"
   printf "             [install]                        \n" 
   printf "             [update]                         \n" 
   printf "             [man]                            \n"
   printf "             [gq]                             \n"
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
ALIEN_GUI()
###########################################################################
{
  SCRIPT=$1

  shift 1
  if [ -x $ALIEN_ROOT/scripts/GUI/$SCRIPT.pl ]
  then
    exec $ALIEN_PERL  $ALIEN_DEBUG $ALIEN_ROOT/scripts/GUI/$SCRIPT.pl $*
  else
    echo "ERROR: Please install Alien-GUI module."
  fi
  exit 
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
ALIEN_RunJob()
###########################################################################
{
  export ALIEN_PROCESSNAME=JOB

  exec $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/Service.pl ProcessMonitor -queueId $*
  exit
}
###########################################################################
ALIEN_RunAgent()
###########################################################################
{
  export ALIEN_PROCESSNAME=JOBAGENT

  exec $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/Service.pl JobAgent $*
  exit
}

##########################################################################
ALIEN_Install()
###########################################################################
{
  $ALIEN_ROOT/scripts/Monitor/Update.sh Kill
  $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/Monitor/Install.pl $*
  if [ -f $ALIEN_ROOT/java/MonaLisa/AliEn/startMonaLisa.pl ]
  then  
    $ALIEN_PERL $ALIEN_ROOT/java/MonaLisa/AliEn/startMonaLisa.pl
  fi
  if [ -f $ALIEN_ROOT/webmin/miniserv.pl ]
  then  
    trap '' 1
    LANG=
    export LANG
    unset PERLIO
    export PERLIO
    mkdir -p /tmp/.webmin/
    exec $ALIEN_PERL $ALIEN_ROOT/webmin/miniserv.pl $ALIEN_ROOT/etc/webmin/miniserv.conf
  fi
}

###########################################################################
ALIEN_Platform()
###########################################################################
{
  echo `uname -s`-`uname -m`-`uname -r | cut -d "." -f 1-2`-gnu-`gcc -dumpversion`;  
}


###########################################################################
ALIEN_CVS_checkout()
###########################################################################
{
  printf "! Please Wait ! \n";
  mkdir -p ~/.alien/software/CVS; 
  test -d ~/.alien/software/CVS  || (echo "Error: cannot create ~/.alien/software/CVS !" && exit -1;)
  cd ~/.alien/software/CVS;
  cvs -d :pserver:cvs:cvs@alisoft.cern.ch:/soft/cvsroot co -AP AliEn 
  cd -;
  exit ;
}

###########################################################################
ALIEN_CVS_update()
###########################################################################
{
  test -d ~/.alien/software/CVS/AliEn  || (echo "Error: ~/.alien/software/CVS does not exist!" && exit -1;)
  cd ~/.alien/software/CVS/AliEn 
  cvs -d :pserver:cvs:cvs@alisoft.cern.ch:/soft/cvsroot update -AdP 
  cd -;
  exit;
}

###########################################################################
ALIEN_CVS_replace()
###########################################################################
{
  printf "! Please Wait ! \n";
  olddir=$PWD;
  test -d ~/.alien/software/CVS/AliEn || (echo "Error: ~/.alien/software/CVS does not exist!" && exit -1;)  
  cd ~/.alien/software/CVS/AliEn; 
  make clean; 
  cd ..; 
  tar czf AliEn-`date +%s`.tgz AliEn; 
  cd ~/.alien/software/CVS; 
  cvs -d :pserver:cvs:cvs@alisoft.cern.ch:/soft/cvsroot co -AP AliEn 
  cd $olddir;
  exit; 
}

###########################################################################
ALIEN_CVS_install()
###########################################################################
{
  printf "! Please Wait ! \n";
  test -d ~/.alien/software/CVS/AliEn || (echo "Error: ~/.alien/software/CVS does not exist!" && exit -1;)  
  test -d ~/.alien/software/Installation || (echo "Error: ~/.alien/software/Installation does not exist! Install the Base System!" && exit -1;)

  cd ~/.alien/software/CVS/AliEn; 
  ~/.alien/software/Installation/alien/bin/alien-perl Makefile.PL;
  make;
  make install;
  cd -;
  exit;
}

###########################################################################
ALIEN_CVS_download()
###########################################################################
{
  # arg 1 : URL where to download from the zipped tar file to install into the CVS directory
  printf "! Please Wait ! \n";
  if [ -z $1 ] 
  then 
    echo "Usage: CVS_download <URL>";
    exit -1;
  fi

  mkdir -p ~/.alien/software/CVS/; 
  test -d ~/.alien/software/CVS || (echo "Error: cannot create ~/.alien/software/CVS !" && exit -1;) 

  cd ~/.alien/software/CVS ; 
  test -f `basename $1` && cp `basename $1` `basename $1`.orig; 
  wget -N -K $1; diff -q `basename $1`  `basename $1`.orig || ( tar xzf `basename $1`;)
  cd -;
  exit ;
}

###########################################################################
ALIEN_CVS_purge()
###########################################################################
{
  printf "! Please Wait ! \n";
    test -d ~/.alien/software/CVS || (echo "Error: ~/.alien/software/CVS does not exist!" && exit -1;)     
    cd ~/.alien/software/CVS;
    rm -f "*.tgz";
    cd -;
    exit ;
}

###########################################################################
ALIEN_MODULE_install()
###########################################################################
{
  # arg 1: Module Name f.e. Monitor
  printf "! Please Wait ! \n";
  cd ~;
  mkdir -p ~/.alien/software/Installation/; 
  test -d ~/.alien/software/Installation || (echo "Error: cannot create ~/.alien/software/Installation !" && exit -1;)  

  cd ~/.alien/software/Installation; 

  test -f AliEn-$1.tar.gz && cp AliEn-$1.tar.gz AliEn-$1.tar.gz.orig; 
  wget -N -K http://alien.cern.ch/download/current/`uname -s`-`uname -m`-`uname -r | cut -d "." -f 1-2`-gnu-`gcc -dumpversion`/AliEn-$1.tar.gz;  
  ( tar xzf AliEn-$1.tar.gz; )
  cd -;
  exit;
}

###########################################################################
ALIEN_BASE_install()
###########################################################################
{
  printf "! Please Wait ! \n";
  cd ~;
  mkdir -p ~/.alien/software/Installation/; 
  test -d ~/.alien/software/Installation || (echo "Error: cannot create ~/.alien/software/Installation !" && exit -1;)  

  cd ~/.alien/software/Installation; 

  test -f AliEn-Base.tar.gz && cp AliEn-Base.tar.gz AliEn-Base.tar.gz.orig; 
  wget -N -K http://alien.cern.ch/download/current/`uname -s`-`uname -m`-`uname -r | cut -d "." -f 1-2`-gnu-`gcc -dumpversion`/AliEn-Base.tar.gz;  
  ( tar xzf AliEn-Base.tar.gz; ~/.alien/software/Installation/alien/bin/alien-perl --bootstrap;)
  cd -;
  exit;
}

###########################################################################
ALIEN_BASE_update()
###########################################################################
{
  printf "! Please Wait ! \n";
  mkdir -p ~/.alien/software/Installation/; 
  test -d ~/.alien/software/Installation || (echo "Error: cannot create ~/.alien/software/Installation !" && exit -1;)  

  cd ~/.alien/software/Installation; 
  test -f AliEn-Base.tar.gz && cp AliEn-Base.tar.gz AliEn-Base.tar.gz.orig; wget -N -K http://alien.cern.ch/download/current/`uname -s`-`uname -m`-`uname -r | cut -d "." -f 1-2`-gnu-`gcc -dumpversion`/AliEn-Base.tar.gz; 
  diff -q AliEn-Base.tar.gz AliEn-Base.tar.gz.orig || ( tar xzf AliEn-Base.tar.gz; ~/.alien/software/Installation/alien/bin/alien-perl --bootstrap;) 
  cd -;
  exit;
}

###########################################################################
ALIEN_BASE_download()
###########################################################################
{
  # arg 1 : URL where to download from the zipped tar file to install into the base directory

  if [ -z $1 ] 
  then 
    echo "Usage: BASE_download <URL>";
    exit -1;
  fi

  mkdir -p ~/.alien/software/Installation/; 
  test -d ~/.alien/software/Installation || (echo "Error: cannot create ~/.alien/software/Installation !" && exit -1;)  
  
  cd ~/.alien/software/Installation;   

  test -f `basename $1` && cp `basename $1` `basename $1`.orig; 
  wget -N -K $1; diff -q `basename $1`  `basename $1`.orig || ( tar xzf `basename $1`;)
  cd -;
  exit;
}

###########################################################################
ALIEN_BASE_purge()
###########################################################################
{
  printf "! Please Wait ! \n";
  test -d ~/.alien/software/Installation || (echo "Error: ~/.alien/software/Installation does not exist!" && exit -1;)     
  cd ~/.alien/software/Installation;
  pwd
  echo -e "purging ... \n`ls *.orig`";
  rm -f *.orig;
  echo -e "done!";
  cd -;
  exit ;
}

###########################################################################
ALIEN_INSTALLER_install()
###########################################################################
{
  printf "! Please Wait ! \n";
  cd ~;
  mkdir -p ~/.alien/software/Installer/; 
  test -d ~/.alien/software/Installer || (echo "Error: cannot create ~/.alien/software/Installer !" && exit -1;)  

  cd ~/.alien/software/Installer; 
  test -f AliEn-Installer.tar.gz && cp AliEn-Installer.tar.gz AliEn-Installer.tar.gz.orig; 
  wget -N -K http://alien.cern.ch/download/current/`uname -s`-`uname -m`-`uname -r | cut -d "." -f 1-2`-gnu-`gcc -dumpversion`/AliEn-Installer.tar.gz;  
  tar xzf AliEn-Installer.tar.gz | awk '{{printf("\r%d files", a); } a++; } END {print "\r Done ........ "}'; 
  cd -;
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
ALIEN_Update()
###########################################################################
{
  cp $ALIEN_ROOT/scripts/Monitor/Update.sh /tmp/Update.sh.$$
  cd $HOME
  /tmp/Update.sh.$$ $* &
  exit 0
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
ALIEN_CreateKeys()
###########################################################################
{
  exec $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/createKeys.pl $*
}
###########################################################################
ALIEN_UpdateToken()
###########################################################################
{
  exec $ALIEN_PERL $ALIEN_DEBUG $ALIEN_ROOT/scripts/updateToken.pl $*
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
ALIEN_MyProxy()
###########################################################################
{
  SetupCertEnvVars
  if [ -z "$MYPROXY_LOCATION" -o -z "$GLOBUS_LOCATION" ]
  then
    printf "Error: MYPROXY_LOCATION and/or GLOBUS_LOCATION not set.\n"  
    printf "Please (re)run 'alien config' command.\n\n"
    exit 1
  fi
  command=$1
  shift 1
  export MYPROXY_SERVER_PORT=8512
  export MYPROXY_SERVER=alien.cern.ch
  export MYPROXY_SERVER_DN=/DC=ch/DC=cern/OU=computers/CN=alien.cern.ch

  case $command in
      myproxy-info|myproxy-destroy)
       exec $MYPROXY_LOCATION/bin/$command -l $ALIEN_USER $*
        ;;
      myproxy-init)
        exec $MYPROXY_LOCATION/bin/$command -l $ALIEN_USER $* 
        ;;
      *)
        printf "%s: No such file or directory.\n" $command
        exit 1
        ;;    
   esac
}
###########################################################################
ALIEN_Grid()
###########################################################################
{
  SetupCertEnvVars
  if [ -z $GLOBUS_LOCATION ]
  then
    printf "Error: GLOBUS_LOCATION not set.\n"
    printf "Please (re)run 'alien config' command.\n\n"
    exit 1
  fi

  if [ -x $GLOBUS_LOCATION/bin/grid-$1 ]
  then 
    exec $GLOBUS_LOCATION/bin/grid-$*
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
	      alien -no_catalog virtual -exec version
#              printf "%s\n" $VERSION
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
	gas)
	    shift 1
	    export ALIEN_PROCESSNAME="Prompt::GAS"
	    ALIEN_UI="AliEn::EGEE::UI"
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
        xfiles|xjobs)
	    ALIEN_GUI $*
	    ;;
        create-keys)
            shift 1
            ALIEN_CreateKeys $*
            ;;
        update-token)
            shift 1
            ALIEN_UpdateToken $*
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
        myproxy-*)
            ALIEN_MyProxy $*
            ;;
        install)
            shift 1
            ALIEN_Install $*
            ;;
        update)
            shift 1
            ALIEN_Update $*
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
        gq)
            shift 1
            [ -x $ALIEN_ROOT/bin/gq ] && exec $ALIEN_ROOT/bin/gq $*
	    echo "$ALIEN_ROOT/bin/gq does not exist"
	    exit -1
            ;;
        register-cert)
            shift 1
            ALIEN_RegisterCert $*
            ;;
        config|configure)
            shift 1
            ALIEN_Config $*
            ;;
        job-submit)
            shift 1
            ALIEN_RunJob $*
            ;;
        *)
	    ALIEN_ARG="$ALIEN_ARG$1 "
	    shift 1
    esac
  done
    
  ALIEN $ALIEN_ARG$* 
}


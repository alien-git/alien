package AliEn::LQ::LCG;

use AliEn::LQ;
use vars qw (@ISA);
push @ISA,   qw( AliEn::LQ);

use strict;
use AliEn::Database::CE;
use AliEn::Classad::Host;
use Data::Dumper;
use File::Basename;
use Net::LDAP;
use AliEn::TMPFile;
use POSIX ":sys_wait_h";
use Time::Local;

sub getQueueStatus { ##Still return values from the local DB
  my $self = shift;
  my $value = $self->{DB}->queryValue("SELECT COUNT (*) FROM JOBAGENT WHERE status<>'DEAD'");
  $value or $value = 0;
  return $value;
}

#
#---------------------------------------------------------------------
#

sub readCEList {
   my $self = shift;
   $self->{LOGGER}->error("LCG", "No CE list defined in \$ENV") unless $ENV{CE_LCGCE};
   my $string = $ENV{CE_LCGCE};
   my @sublists = ($string =~ /\(.+?\)/g);
   $string =~ s/\($_\)\,?// foreach (@sublists);
   push  @sublists, split(/,/, $string);
   $self->{CONFIG}->{CE_LCGCE_LIST} = \@sublists;
   $self->info("CE list is: @{$self->{CONFIG}->{CE_LCGCE_LIST}}");
   # Flat-out sublist in CE list
   $string = $ENV{CE_LCGCE};
   $string =~  s/\s*//g;
   $string =~ s/\(//g; $string =~ s/\)//g;
   my @flatlist = split /,/,$string;
   $self->{CONFIG}->{CE_LCGCE_FLAT_LIST} = \@flatlist;

   # A list with only the first from each sublist, to avoid double counting when needed
   my @firsts = ();
   my @list = @{$self->{CONFIG}->{CE_LCGCE_LIST}};
   foreach my $CE ( @list ) {
    $CE =~ s/\s*//g; $CE =~ s/\(//; $CE =~ s/\)//;
     ($CE, undef) = split (/,/,$CE,2);
     push @firsts,$CE;
   }
   $self->{CONFIG}->{CE_LCGCE_FIRSTS_LIST} = \@firsts;
   return 1;
}


sub queryBDII {
  my $self = shift;
  my $CE = shift;
  my $filter = shift;
  $filter or $filter = "objectclass=*";
  my $base = shift;
  $base or $base = "GlueVOViewLocalID=\L$self->{CONFIG}->{LCGVO}\E,GlueCEUniqueID=$CE";
  my @items = @_;
  my %results = ();
  $self->info("Querying $CE for @items");
  $self->debug(1,"DN string is $base");
  $self->debug(1,"Filter is $filter");
  (my $host,undef) = split (/:/,$CE);    
  my $IS  = "ldap://$host:2170,mds-vo-name=resource,o=grid"; # Resource BDII
  $IS = $self->{CONFIG}->{CE_SITE_BDII} if ( defined $self->{CONFIG}->{CE_SITE_BDII} );  
  my $ldap = '';
  my ($GRIS, $BaseDN) = split (/,/,$IS,2);
  $self->debug(1,"Asking $GRIS/$BaseDN");
  unless ($ldap =  Net::LDAP->new($GRIS)) {
    $self->info("$GRIS/$BaseDN not responding (1)");
    return;
  }
  unless ($ldap->bind()) {
    $self->{LOGGER}->info("$GRIS/$BaseDN not responding (2)");
    next;
  }
  my $result = $ldap->search( base   => "$base,$BaseDN",
                              filter => "$filter");
  my $code = $result->code;                           
  my $msg = $result->error;                           
  if ($code) {
    $self->{LOGGER}->warning("LCG","\"$msg\" ($code) from $GRIS/$BaseDN");
    return;
  }
  if ( ($result->all_entries)[0] ) {
    foreach (@items) {
      my $value = (($result->all_entries)[0])->get_value("$_");
      $self->debug(1, "$_ for $CE is $value");
      $results{$_} = $value;
    }
    my $message = "OK, got an answer from $GRIS/$BaseDN: ";
    $message = $message.$results{$_}." " foreach keys %results;
    $self->info($message);
  } else {
    $self->{LOGGER}->warning("LCG","The query to $GRIS/$BaseDN did not return any value");
    return;
  }
  $ldap->unbind();
  $self->debug(1,"Returning: ".Dumper(\%results));
  return \%results;
}

sub getCEInfo {
  my $self = shift;
  my @items = @_;
  my %results = ();
  my $someAnswer = 0;
  $self->debug(1,"Querying all CEs, requested info: @items");
  my @list = @{$self->{CONFIG}->{CE_LCGCE_LIST}};
  foreach my $CE ( @list ) {
    $self->info("Querying for $CE");
    if (  $CE =~ m/\(.*\)/ ) { #It's a sublist, get the max value for each value
      $CE =~ s/\s*//g; $CE =~ s/\(//; $CE =~ s/\)//;
      my @sublist = split /,/, $CE;
      my %max = ();
      foreach my $subCE (@sublist) {
        $self->info("In the sublist, querying for $subCE");
        my $res = $self->queryBDII($subCE,'',"GlueVOViewLocalID=\L$self->{CONFIG}->{LCGVO}\E,GlueCEUniqueID=$subCE",@_);
        if ( $res ) {
          foreach (@items) {
            if ($res->{$_} =~ m/44444/) {
              $self->{LOGGER}->warning("LCG","Query for $subCE gave 44444.");
              next;
            }
            $max{$_} = $res->{$_} if (!defined  $max{$_} || $res->{$_}>$max{$_});
          }
        } else { 
          $self->{LOGGER}->warning("LCG","Query for $CE failed.");
          next;
        }   
      }   
      foreach (@items) {
        $results{$_} += $max{$_} if defined $max{$_};
      }
    } else {
      (my $host,undef) = split (/:/,$CE);    
      my $res = $self->queryBDII($CE,'',"GlueVOViewLocalID=\L$self->{CONFIG}->{LCGVO}\E,GlueCEUniqueID=$CE",@_);
      if ( $res ) {
        $results{$_} += $res->{$_} foreach (@items);
      } else { 
        $self->{LOGGER}->warning("LCG","Query for $CE failed.");
        next;
      }   
    }
   }    
  my @values = ();
  foreach (@items) {
    $results{$_} = 44444 unless defined $results{$_};
    push (@values,$results{$_});
  }
  $self->debug(1,"Returning: ".Dumper(@values));
  return @values;
}

sub getJobStatus {
   my $self = shift;
   my $contact = shift;
   my $pattern = shift;
   $contact or return;
   $self->info("Getting status from LCG for $contact");
   $pattern or $pattern = 'Current Status:';
   my $user = getpwuid($<);
   my @args=();
   $self->{CONFIG}->{CE_STATUSARG} and
     @args=split (/\s+/, $self->{CONFIG}->{CE_STATUSARG});
   my $logfile = AliEn::TMPFile->new({ ttl => '12 hours'});

   my @output=$self->_system($self->{STATUS_CMD}, "-noint", "--logfile", $logfile, @args,
                             "\"$contact\" | grep \"$pattern\"" );
   my $status = $output[0];
   $status or return;
   chomp $status;
   $status =~ s/$pattern//;
   $status =~ s/ //g;
   return $status;
}

sub getContactByQueueID {
   my $self = shift;
   my $queueid = shift;
   $queueid or return;
   my $contact = '';
   return $contact;
}

sub renewProxy {
   my $self = shift;
   $self->{INITIME}=time; 
   my $duration = shift;
   $duration or $duration=$self->{CONFIG}->{CE_TTL};
   $duration or $duration = 172800; #in seconds
   my $thres = shift;
   $thres or $thres = 0;
   $self->info("Checking whether to renew proxy for $duration seconds");
   $ENV{X509_USER_PROXY} and $self->debug(1,"\$X509_USER_PROXY is $ENV{X509_USER_PROXY}");
   my $ProxyRepository = "$self->{CONFIG}->{VOBOXDIR}/proxy_repository";
   my $command = "vobox-proxy --vo \L $self->{CONFIG}->{LCGVO}\E query";
   
   my @lines = $self->_system($command);
   my $dn = '';
   my $proxyfile = '';
   my $timeLeft = '';
   foreach (@lines) {
     chomp;
     m/^DN:/ and ($dn) = m/^DN:\s*(.+)\s+$/;
     m/^File:/ and ($proxyfile) = m/^File:\s*(.+)\s+$/;
     m/^Proxy Time Left/ and ($timeLeft) = m/^Proxy Time Left \(seconds\):\s*(.+)\s+$/;
   }
   $dn or $self->{LOGGER}->error("LCG","No valid proxy found.") and return;
   $self->debug(1,"DN is $dn");
   $self->debug(1,"Proxy file is $proxyfile");
   $self->info("Proxy timeleft is $timeLeft (threshold is $thres)");
   return 1 if ( $thres>0 && $timeLeft>$thres );
   # I apparently cannot pass this via an argument
   my $currentProxy = $ENV{X509_USER_PROXY};
   $self->{LOGGER}->warning("LCG","\$X509_USER_PROXY different from the proxy we are renewing") if ($currentProxy ne $proxyfile);
   $self->{LOGGER}->warning("LCG","$currentProxy and $proxyfile ") if ($currentProxy ne $proxyfile);
   $ENV{X509_USER_PROXY} = "$self->{CONFIG}->{VOBOXDIR}/renewal-proxy.pem";
   $self->debug(1,"Renewing proxy for $dn for $duration seconds");
   my @command=("$ENV{LCG_LOCATION}/bin/lcg-proxy-renew", "-a", "$proxyfile",
	        "-d", "-t",int($duration/3600).":", #in hours
	        "-o", "/tmp/tmpfile.$$" , "--cert", $ENV{X509_USER_PROXY}, 
	        "--key", $ENV{X509_USER_PROXY});
   my $oldPath=$ENV{PATH};
   my $pattern="$ENV{ALIEN_ROOT}"."[^:]*:";
   $ENV{PATH}=~ s/$pattern//g;
   unless ( $self->_system(@command) ) {
     $ENV{PATH}=$oldPath;
      $self->{LOGGER}->error("LCG","unable to renew proxy");
      $ENV{X509_USER_PROXY} = $currentProxy;
      return;
   }
   $ENV{PATH}=$oldPath;

   @command = ("mv", "-f", "/tmp/tmpfile.$$", "$proxyfile");
   if ( $self->_system(@command) ) {
     $self->{LOGGER}->error("LCG","unable to move new proxy");
     $ENV{X509_USER_PROXY} = $currentProxy;
     return;
   }  
   $command = "vobox-proxy --vo \L$self->{CONFIG}->{LCGVO}\E --dn \'$dn\' query-proxy-timeleft";
   ( my $realDuration ) = $self->_system($command);
   chomp $realDuration;
   $self->{LOGGER}->error("LCG","asked for $duration sec, got only $realDuration") if ( $realDuration < 0.9*$duration);
   $ENV{X509_USER_PROXY} = $currentProxy;

   return 1;
}

sub updateClassAd {
  my $self = shift;
  $self->debug(1,"Updating host classad from IS...");
  my $classad = shift;
  $classad or return;
  my ($maxRAMSize, $maxSwapSize) = (0,0);
  foreach my $CE (@{$self->{CONFIG}->{CE_LCGCE_FLAT_LIST}}) {
    $self->debug(1,"Getting RAM and swap info for $CE");
    my $res = $self->queryBDII($CE,'',"GlueCEUniqueID=$CE",'GlueForeignKey');
    $res or return $classad;
    my $cluster = $res->{'GlueForeignKey'};
    $cluster =~ s/^GlueClusterUniqueID=//;
    $self->debug(1,"Cluster name from IS is $cluster");
    $res = $self->queryBDII($CE,'(GlueHostMainMemoryRAMSize=*)',"GlueClusterUniqueID=$cluster",qw(GlueHostMainMemoryRAMSize GlueHostMainMemoryVirtualSize));
    $res or return $classad;
    $maxRAMSize  = $res->{'GlueHostMainMemoryRAMSize'}  if ($res->{'GlueHostMainMemoryRAMSize'}>$maxRAMSize );
    $maxSwapSize = $res->{'GlueHostMainMemoryVirtualSize'} if ($res->{'GlueHostMainMemoryVirtualSize'}>$maxSwapSize );
  }  
  $self->{UPDATECLASSAD} = time();    
  $self->info("Updating host ClassAd from IS (RAM,Swap) = ($maxRAMSize,$maxSwapSize)" );
  $classad->set_expression("Memory",$maxRAMSize*1024);
  $classad->set_expression("Swap",$maxSwapSize*1024);
  $classad->set_expression("FreeMemory",$maxRAMSize*1024);
  $classad->set_expression("FreeSwap",$maxSwapSize*1024);
  return $classad;
}

sub translateRequirements {
  my $self = shift;
  my $ca = shift;
  my $requirements= shift || "";;

  $ca or return $requirements;

  my ($ok, $memory) =  $ca->evaluateAttributeString("Memory");
  if ($memory) {
    $self->info("Translating \'Memory\' requirement ($ok,$memory)");
    $requirements .= "&& other.GlueHostMainMemoryRAMSize>=$memory";
  }
  ($ok, my $swap) =  $ca->evaluateAttributeString("Swap");
  if ($swap) {
    $self->info("Translating \'Swap\' requirement ($ok,$swap)");
    $requirements .= "&& other.GlueHostMainMemoryVirtualSize>=$swap";
  }
  ($ok,my  $ttl)= $ca->evaluateAttributeString("Requirements");
  if ($ttl and $ttl =~ /TTL\s*[=>]*\s*(\d+)/ ) {
     $self->info("Translating \'TTL\' requirement ($1)");
     $requirements .= "&& other.GlueCEPolicyMaxWallClockTime>=".$1/60; #minutes
   }
  return $requirements;
}

sub generateJDL {
  return;
}

sub installWithParrot {
  my $self=shift;

  return "\$HOME/bootsh /opt/alien/bin/alien", "rm -rf bootsh
wget -O bootsh http://alien.cern.ch/bootsh
chmod +x bootsh
export PATH=/opt/alien/bin:\$PATH\n";
}

sub installWithTorrent {
  my $self=shift;
  $self->info("The worker node will install with the torrent method!!!");

  return "$self->{CONFIG}->{TMP_DIR}/alien_installation.\$\$/alien/bin/alien","DIR=$self->{CONFIG}->{TMP_DIR}/alien_installation.\$\$
mkdir -p \$DIR
echo \"Ready to install alien\"
date
cd \$DIR
wget http://alien.cern.ch/alien-torrent-installer -O alien-auto-installer
export ALIEN_INSTALLER_PREFIX=\$DIR/alien
chmod +x alien-auto-installer
./alien-auto-installer -skip_rc  -type workernode -batch
echo \"Installation completed!!\"

";
}

sub installWithLocal {
  my $self=shift;
  my $version=$self->{CONFIG}->{VERSION};
  $version=~ s{\..*$}{};
  my $vo_dir="VO_".uc($self->{CONFIG}->{ORG_NAME})."_SW_DIR";

  return "alien", "IDIR=\$HOME/alien_auto_install

if [ -n \"\$$vo_dir\" ]
then
    echo \"Let's try to use $vo_dir=\$$vo_dir\"
    [ -d \$$vo_dir ] || mkdir \$$vo_dir
    touch \$$vo_dir/user.\$UID.lock
    if [ \$? = \"0\" ]
    then
      echo 'The lock worked!! :)'
      rm \$$vo_dir/user.\$UID.lock
      IDIR=\$$vo_dir/alien_auto_install
    fi
fi


if [ -e \$IDIR/lock ]
then
    echo \"The lock \$IDIR/lock exists. Is anybody installing alien?\"
    echo \"Let's exit so that we do not interfere\"
    exit -2
fi
IDIR=\$IDIR/$version
if  [ -d \$IDIR  ]  && [ -f \$IDIR/bin/alien ]
then
    echo \"The installation already exists\"
else

    echo \"Let's install everything\" 
    touch \$IDIR/lock
    rm -rf alien-installer


    wget -O alien-installer http://alien.cern.ch/alien-installer
    chmod +x alien-installer
    mkdir -p \$HOME/.alien \$IDIR \${IDIR}_cache

    case `uname -m` in
      i*86*)
        PLATFORM=i686-pc-linux-gnu
       ;;
      x86_64)
        PLATFORM=x86_64-unknown-linux-gnu
        ;;
      powerpc)
        PLATFORM=powerpc-apple-darwin8.1.0
        ;;
      apple*)
        PLATFORM=i686-apple-darwin8.6.1
        ;;
      ia64)
        PLATFORM=ia64-unknown-linux-gnu
        ;;
       *)
        echo 'Unknown or unsupported platform: ' `uname -m`
        exit 1
        ;;
    esac


    wget http://alien.cern.ch/BitServers -O BitServers

    echo \"This platform is \$PLATFORM\"
    URL=`grep -v -e '^#' BitServers | grep \$PLATFORM  |awk -F \\| '{print \$2}'| awk '{print \$1}'`
    echo \"It will download from \$URL\"

    cat  >\$HOME/.alien/installer.rc <<EOF
ALIEN_INSTALLER_HOME=\${IDIR}_cache
ALIEN_INSTALLER_PREFIX=\$IDIR
ALIEN_INSTALLER_AUTODETECT=false
ALIEN_INSTALLER_TYPE=wn
ALIEN_INSTALLER_PLATFORM=\$PLATFORM
ALIEN_DIALOG=dialog
ALIEN_RELEASE=$version
ALIEN_BITS_URL=\$URL/\$PLATFORM/$version/download/
EOF
    echo \"Starting the installation\"
    date
    ./alien-installer --trace   update
    if [  $? ne 0 ]
    then
       echo \"The installer wasn't happy. Removing the installation\"
       rn -rf \$IDIR
    fi  
    rm \$IDIR/lock
    echo \"Installation finished!!\"
    date
    ls \$IDIR
    echo \"And the size\"
    du -sh \$IDIR
fi
export ALIEN_ROOT=\$IDIR
export PATH=\$ALIEN_ROOT/bin:\$PATH
";
}

sub _system {
  my $self=shift;
  my $command=join (" ", @_);

  my $pid;
  local $SIG{ALRM} =sub {
    print "$$ timeout while doing '$command'\n";
    $pid and print "Killing the process $pid\n" and CORE::kill(9, $pid);

    print "Let's try to close the file handler\n";
    close FILE;
    print " $$ File closed";

    die("timeout!! ");
  };
  my @output;
  eval {
    alarm(300);
    $self->setEnvironmentForLCG();
    $self->info("Doing '$command'");
    $pid=open(FILE, "$command |") or
      die("Error doing '$command'!!\n$!");
    @output=<FILE>;

    if (! close FILE){
      #We have to check that the proces do^?^?
      print "The system call failed  PID $pid\n";
      if (CORE::kill 0,$pid) {
        my $kid;
        do {
          $kid = waitpid($pid, WNOHANG);
        }   until $kid > 0;
      }
    }
    alarm(0);
  };
  my $error=$@;
  $self->unsetEnvironmentForLCG();
  if ($error) {
    $self->info("Error: $error");
    close FILE;
    $pid and print "Killing the process $pid\n" and CORE::kill(9, $pid);
    alarm(0);
    return;
  }
  return @output;
}
sub setEnvironmentForLCG{
  my $self=shift;

  $self->info("Setting the environment for an LCG call");
  $self->{LCG_ENV}={};
  foreach  my $v ("GLOBUS_LOCATION", "X509_CERT_DIR", "MYPROXY_LOCATION"){
    $self->{LCG_ENV}->{$v}=$ENV{$v};
    delete $ENV{$v};
  }
  $self->{LCG_ENV}->{PATH}=$ENV{PATH};

  $ENV{PATH}=~ s/$ENV{ALIEN_PATH}//;
  $self->{LCG_ENV}->{LD_LIBRARY_PATH}=$ENV{LD_LIBRARY_PATH};

  $ENV{LD_LIBRARY_PATH}=~ s/$ENV{ALIEN_LD_LIBRARY_PATH}//;


  $ENV{GLOBUS_LOCATION}="/opt/globus";
}
sub unsetEnvironmentForLCG{
  my $self=shift;
  $self->info("Back to the normal environment");
  foreach my $v (keys %{$self->{LCG_ENV}}){
    $ENV{$v}=$self->{LCG_ENV}->{$v};
  }
}

sub generateStartAgent{
  my $self=shift;
  my $command=shift;

  my $exeFile = AliEn::TMPFile->new({filename=>"dg-submit.$$.sh"})
    or return;


  open( BATCH, ">$exeFile" )
    or print STDERR "Can't open file '$exeFile': $!"
      and return;
  print BATCH "\#!/bin/sh
\# Script to run AliEn on LCG
\# Automatically generated by AliEn running on $ENV{HOSTNAME}

export OLDHOME=\$HOME
export HOME=`pwd`
export ALIEN_LOG=$ENV{ALIEN_LOG}
echo --- hostname, uname, whoami, pwd --------------
hostname
uname -a
whoami
pwd
echo --- ls -la ------------------------------------
ls -lart
echo --- df ----------------------------------------
df -h
echo --- free --------------------------------------
free

";

  my $exec="alien";
  $self->info("Writing the command that we got from the standard method: $command");
  my $info=$command;
  if (-f $command){
    open (FILE, "<$command") or $self->info("Error opening $command!") and return;
    $info=join("",<FILE>);
    close FILE;
  }


  print BATCH "$info\n";
  print BATCH "
cd \${TMPDIR:-.}
echo --- env ---------------------------------------
echo \$PATH
echo \$LD_LIBRARY_PATH

echo --- alien --printenv --------------------------
$exec -printenv
echo --- alien proxy-info ---------------------------
$exec proxy-info
echo --- Run ---------------------------------------
ls -lart
";

  close BATCH or return;
  return $exeFile;
}



return 1;

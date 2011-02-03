# Utility Functions for AliEn
# AJP 07/2004
# ----------------------------------------------------------

package AliEn::Util;

use strict;
use POSIX ":sys_wait_h";
require AliEn::Database::Util;
sub textneutral    { return "\033[0m";}
sub textblack      { return "\033[49;30m";}
sub textred        { return "\033[49;31m";}
sub textrederror   { return "\033[47;31m";}
sub textblueerror  { return "\033[47;31m";}
sub textgreen      { return "\033[49;32m";}
sub textyellow     { return "\033[49;33m";}
sub textblue       { return "\033[49;34m";}
sub textbold       { return "\033[1m";}
sub textunbold     { return "\033[0m";}

sub isMac {
    my $platform = `uname`;
    chomp($platform);
    return ($platform eq "Darwin");
}

sub getProcDir {
  my $user = shift;
  my $submitHost = shift;
  my $ID = shift;

  unless ($user) {
    ($user) = split '@', $submitHost;
  }

#  print "\tAliEn::Util::getProcDir called from " . join (" ", caller()) . " with $user, $ID\n";

  #return "/proc/$ID";
  return "~/alien-job-$ID";
}

sub getJobUserByUI {
  my $UI = shift;
  my $ID = shift;

  my ($result) = $UI->execute("ps", "-j", "-id=$ID", "-a" ,"-A");
  $result
    or return;

  my @result = split "###", $result;
  $#result >= 4
    and $result[4]
    or return;

  my ($user) = split '@', $result[4];

  return $user;
}

sub getJobUserByDB {
  my $DB = shift;
  my $ID = shift;

  my $result = $DB->getFieldFromQueue($ID, "submitHost");
  $result
    or return;

  my ($user) = split '@', $result;

  return $user;
}

sub JobStatus {
  return ['ASSIGNED','DONE','DONE_WARN','ERROR_A','ERROR_I','ERROR_E','ERROR_IB','ERROR_M','ERROR_R','ERROR_S','ERROR_SPLT', 'ERROR_SV','ERROR_V','ERROR_VN','EXPIRED','FAILED','FORCEMERGE','IDLE','INSERTING','INTERACTIV','KILLED','MERGING','QUEUED','RUNNING','SAVING','SAVED','SAVED_WARN', 'SPLIT','SPLITTING','STARTED','WAITING','ZOMBIE', 'ERROR_VT', 'TO_STAGE', 'STAGING', 'A_STAGED', 'OVER_WAITING'];
}

# mapping between job status as text and number
my $ml_status ={'INSERTING' => 1,
		'SPLITTING' => 2, 
		'SPLIT' => 3,
		'QUEUED' => 4,
		'WAITING' => 5, 
		'ASSIGNED' => 6,
		'STARTED' => 7,
		'INTERACTIV' => 8, 
		'IDLE' => 9,
		'RUNNING' => 10, 
		'SAVING' => 11,
		'SAVED' => 12,
		'MERGING' => 13, 'FORCEMERGE' => 14,
		'DONE' => 15,
		'DONE_WARN' => 16,
		'TO_STAGE'=>17, 'A_STAGED'=>18,'STAGING'=>19,
		'OVER_WAITING'=>21,
		'ERROR_A' => -1, 'ERROR_I' => -2, 'ERROR_E' => -3, 'ERROR_IB' => -4, 
		'ERROR_M' => -5, 'ERROR_R' => -6, 'ERROR_S' => -7, 'ERROR_SPLT' => -8, 
		'ERROR_SV' => -9, 'ERROR_V' => -10, 'ERROR_VN' => -11, 'EXPIRED' => -12,
		'FAILED' => -13, 'KILLED' => -14, 'ZOMBIE' => -15, 'ERROR_VT' => -16};

# convert a job status to a number to be used in MonaLisa
sub statusForML {
  my $stat = shift;
  #print "statusForML ($stat) => $status->{$stat}\n";
  return $ml_status->{$stat} || 0;
}


my $ml_transferStatus={'INSERTING' => 1,
		       'WAITING' => 2, 
		       'ASSIGNED' => 3,
		       'LOCAL COPY' => 4,
		       'TRANSFERRING' => 5, 
		       'CLEANING' => 6,
		       'DONE' => 7, 
		       'FAILED' => -1,
		       'KILLED' => -2,
		       'EXPIRED' => -3,
		      };

sub transferStatusForML {
  my $stat=shift;
  return $ml_transferStatus->{$stat} || 0;
  
}

my $ja_status ={'REQUESTING_JOB' => 1,
		'INSTALLING_PKGS' => 2,
		'JOB_STARTED' => 3,
		'RUNNING_JOB' => 4,
		'DONE' => 5,
		'ERROR_HC' => -1,      # error in getting host classad
		'ERROR_IP' => -2,      # error installing packages
		'ERROR_GET_JDL' => -3, # error getting jdl
		'ERROR_JDL' => -4,     # incorrect jdl
		'ERROR_DIRS' => -5,    # error creating directories, not enough free space in workdir
		'ERROR_START' => -6,   # error forking to start job
		};

# convert a JobAgent status to a number to be used in ML
sub jaStatusForML {
  my $stat = shift;
  return $ja_status->{$stat} || 0;
}

sub Confirm($) {
    my $text = shift;

    printf STDOUT "$text [<y/n>]\n";
    my $BSD = -f '/vmunix';
    if ($BSD) {
	system "stty cbreak </dev/tty >/dev/tty 2>&1";
    }
    else {
	system "stty", '-icanon';
	system "stty", 'eol', "\001"; 
        }

    my $key = getc(STDIN);

    if ($BSD) {
	system "stty -cbreak </dev/tty >/dev/tty 2>&1";
    }
    else {
	system "stty", 'icanon';
	system "stty", 'eol', '^@'; # ascii null
    }
    print "\n";
    if ($key eq "y") {
	return 1;
    } else {
	return 0;
    }
}

sub setCacheValue{
  my $self=shift;
  my $name=shift;
  my $value=shift;
  my $date=time;

  $self->{CACHE} or $self->{CACHE}={BUILT=>$date};
  if (!defined $value){
    delete $self->{CACHE}->{$name};
    return 1;
  }
  $self->{CACHE}->{$name}={expired=>$date+6000, 
                           timeChecked=>$date,
                           value=>$value};
# $self->debug(1,"Setting the cache of $name = $value at $date");
  return 1;
}
sub returnCacheValue {
  my $self=shift;
  my $name=shift;
  # $self->debug(1,"Checking if we can return a value from the cache ($name)");
  $self->{CACHE}->{$name} or return;
  $self->debug(2,"The entry exists!!");
  my $now=time;
  if ($self->{CACHE}->{$name}->{expired}<$now){
    delete $self->{CACHE}->{$name};
    return;
  }
  $self->debug (1,"The date is correct");
  if ($self->{INST_DIR} and -f "$self->{INST_DIR}/REBUILDCACHE") {
    my @stat=stat("$self->{INST_DIR}/REBUILDCACHE");
    if ($stat[9] > $self->{CACHE}->{BUILT}) {
      $self->info("The cache has been deleted");
      $self->{CACHE}={BUILT=>$now};
      return;
    }
  }
  $self->debug(1,"Returning the value from the cache ($self->{CACHE}->{$name}->{value})");
  return $self->{CACHE}->{$name}->{value};

}

sub returnFileCacheValue{
  my $self=shift;
  $self->{CACHE_DB} or $self->{CACHE_DB}=AliEn::Database::Util->new() or return;
  return $self->{CACHE_DB}->returnCache(@_);
}
sub setFileCacheValue{
  my $self=shift;
  $self->{CACHE_DB} or $self->{CACHE_DB}=AliEn::Database::Util->new() or return;
  return $self->{CACHE_DB}->setCache(@_);

}
  
sub deleteCache {
  my $self=shift;
  my $date=time;
  delete $self->{CACHE};
  $self->{INST_DIR} or return 1;
  system ("touch $self->{INST_DIR}/REBUILDCACHE");

  $self->debug(1, "Deleting the cache at $date");
  sleep(1);
  return 1;
}



# Setup ApMon if MonaLisa configuration is defined in LDAP
sub setupApMon {
  my $self = shift;

  if($self->{CONFIG}->{MONALISA_HOST} || $self->{CONFIG}->{MONALISA_APMONCONFIG}) {
    eval "require ApMon";
    if($@){
      $self->info("ApMon module is not installed; skipping monitoring");
      return;
    }
    my $apmon = ApMon->new(0);
    $apmon->setLogLevel('WARNING');
    if($self->{CONFIG}->{MONALISA_APMONCONFIG}){
    	my $cfg = eval($self->{CONFIG}->{MONALISA_APMONCONFIG});
        $apmon->setDestinations($cfg);
	if(ref($cfg) eq "HASH"){
		my @k = keys(%$cfg);
		$cfg = $k[0];
	}elsif(ref($cfg) eq "ARRAY"){
		$cfg = $$cfg[0];
	}
	$ENV{APMON_CONFIG} = $cfg;
    }else{
    	my $cfg = $self->{CONFIG}->{MONALISA_HOST};
        $apmon->setDestinations([$cfg]);
	$ENV{APMON_CONFIG} = $cfg;
    }
    $apmon->setLogLevel('INFO');

    $self->{MONITOR} = $apmon;
  }
}



sub setupApMonService {
  my $self=shift;
  my $name=shift || "";

  $self->{MONITOR} or return;

  $self->{MONITOR}->setMonitorClusterNode($self->{CONFIG}->{SITE}.'_Nodes', $self->{HOST});
  my $service="";
  my $address=$self->{CONFIG}->{HOST};
  my $fullName="$self->{CONFIG}->{SITE}_$name";
  if ($self->{SERVICE}){
    $service = "ClusterMonitor_" if $self->{SERVICE} eq "ClusterMonitor";
    $service = "SE_" if $self->{SERVICE} eq "SE";
    $address="$self->{HOST}:$self->{PORT}";
    $fullName="$self->{CONFIG}->{SITE}_$service$self->{SERVICENAME}";
    
  }
  $self->{MONITOR}->addJobToMonitor($$, '', $fullName, $address);
  $self->{MONITOR}->sendParameters($fullName, $address); 

    # afterwards I'll use just sendParams regardless of service name
  return 1;
}

sub _system {
  my $command=join (" ", @_);
  local $SIG{ALRM} =sub {
    print "$$ timeout while doing '$command'\n";
    die("timeout!! ");
  };
  my @output;
  eval {
    alarm(300);
    my $pid=open(FILE, "$command |") or
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
  if ($@) {
    print "Error: $@";
    alarm(0);
    return;
  }
  return @output;
}

# Kill all the processes started from the given one or in the same group with it
# NOTE: to use it properly, make sure that the process you kill is in a sepparate
# process group. You can set that by calling "POSIX::setpgid($$, 0);" in it and then
# you give its pid to this function to kill it and all its children.
sub kill_really_all {
  my $pid = shift;
	
  print "Killing all processes beyond $pid...\n";
  my $procmap = {};
  my @tokill = ($pid);
  my @killed = ();
  if(open(PS, "ps -A -eo pid,ppid,pgrp |")){
    <PS>;	# skip header
    while(<PS>){
      s/^\s+//;
      my ($pid,$ppid,$pgrp) = split(/\s+/, $_);
      my @list = ($pid, $pgrp);
      if($procmap->{$ppid}){
	push(@list, @{$procmap->{$ppid}});
      }
      $procmap->{$ppid} = \@list;
      
      my @list2 = ($pid, $pgrp);
      if($procmap->{$pgrp}){
	push(@list2, @{$procmap->{$pgrp}});
      }
      $procmap->{$pgrp} = \@list2;
    }
    close(PS);	
  }else{
    print "kill_really_all: Cannot run PS!!!\n";
  }
  #First, let's give them a warning.
  foreach my $ptk (@tokill){
    next if($ptk == $$);
    next if(grep(/^$ptk$/, @killed));
    kill (11, $ptk);
  }
  sleep(3); 
  while(@tokill){
    my $ptk = shift @tokill;
    next if($ptk == $$);
    next if(grep(/^$ptk$/, @killed));
    kill(9, $ptk);
    push(@tokill, @{$procmap->{$ptk}}) if($procmap->{$ptk});
    push(@killed, $ptk);
  }
  print "Killed procs: @killed\n";
}

# Compute the total jiffies for the given pid and all its children.
# This value can be used in regular checks to see if a process (and its 
# children) got stuck or they continue to run, since jiffies are the
# finest accountable unit for a proces.
sub get_pid_jiffies {
	my $pid = shift;

	my $sum = 0;
	my $procmap = {};
	my @tocheck = ($pid);
	if(open(PS, "ps -A -eo pid,ppid |")){
		<PS>;	# skip header
		while(<PS>){
			s/^\s+//;
			my ($pid, $ppid) = split(/\s+/, $_);
			if($procmap->{$ppid}){
				push(@{$procmap->{$ppid}}, $pid);
			}else{
				$procmap->{$ppid} = [$pid];
			}
		}
		close(PS);
	}else{
		print "get_pid_jiffies: Cannot run PS!!\n";
	}
	while(@tocheck){
		my $ptc = shift @tocheck;
		push(@tocheck, @{$procmap->{$ptc}}) if($procmap->{$ptc});
		if(open(PROC, "/proc/$ptc/stat")){
			my @fields = split(/\s+/, <PROC>);
			$sum += $fields[13] + $fields[14];
			close(PROC);
		} # else the process is already gone
	}
	return $sum;
}


sub getPlatform
{
  my $self=shift;
  $self and  $self->{PLATFORM_NAME} and return $self->{PLATFORM_NAME};

  my $config;
  $self and $self->{CONFIG} and $config=$self->{CONFIG};
  $config or $config=AliEn::Config->new();
  $config or print STDERR "Error getting the configuration to check the platform\n" and return;
  if ($config->{PACKMAN_PLATFORM}){
    $self->{PLATFORM_NAME}=$config->{PACKMAN_PLATFORM};
    return $config->{PACKMAN_PLATFORM};
  }
    
  my $sys1 = `uname -s`;
  chomp $sys1;
  $sys1 =~ s/\s//g; #remove spaces
  my $sys2 = `uname -m`;
  chomp $sys2;
  $sys2 =~ s/\s//g; #remove spaces
  my $platform="$sys1-$sys2";
  
  $self and $self->{PLATFORM_NAME}=$platform;
  return $platform;
}

sub mkdir{
  my $dir=shift;
  my $mode=shift || 0755;
  if (! -d $dir){
    my $dir2="";
    foreach ( split ( "/", $dir ) ) {
      $dir2 .= "/$_";
      (-d $dir2) and next;
      mkdir $dir2, $mode;
    }
  }
  (-d $dir) and return 1;
  return;
}

sub find_memory_consumption {
  my $apmon=shift; 
  my $id=shift;
  my @requestSet=("virtualmem");
  
  my @resultSet=$apmon->{BG_MONITOR}->{PROC_INFO}->getJobData($id,\@requestSet); 
  my $totalMem=0;
  if(@resultSet){
     if($resultSet[0] eq $requestSet[0] ){
      $totalMem=$resultSet[1];
     }
  }
  return $totalMem;
}


sub isValidSEName{
   my $se=shift;
   ($se eq "no_se") and return 1;
   my @entries = split(/\:\:/,$se);
   my $isOk = (scalar(@entries) eq 3) ;
   foreach (@entries) { $isOk = ($isOk and ($_ =~ /^[0-9a-zA-Z_\-]+$/)) ; }
   return $isOk;
}

sub isValidGUID{
   my $guid=shift;
   my $lines = $guid;
   # guid has to be 36 chars long, containing 4 times '-', at position 9,14,19,24 and the rest needs to be hexdec
   (length($guid) eq 36)
     and $lines = substr($lines, 8, 1)
        .substr($lines, 13, 1).substr($lines, 18, 1).substr($lines, 23, 1)
     and $lines =~ s/[-]*// and (length($lines) eq 0)
     and $guid  = substr($guid, 0, 8)
        .substr($guid, 9, 4).substr($guid, 14, 4)
        .substr($guid, 19, 4).substr($guid, 24, 12)
     and $guid =~ s/[0-9a-f]*//i
     and (length($guid) eq 0)
     and return 1;
     return 0;
}

sub isValidPFN{
   my $pfn=shift;

   $pfn =~ /^[a-zA-Z]+\:\/\/[a-zA-Z0-9\-\.]+(\:[a-zA-Z0-9]*)?\/\// and return 1;

   return 0; 
}

  
sub findAndDropArrayElement{
  my $tag=(shift || return (0,[]));
  my $back=0;
  my @list=();
  foreach (@_){
     ($_ eq $tag) and $back=1 or push @list, $_;
  }
  return ($back, \@list);
}

sub getValFromEnvelope {
  my $env=(shift || return 0);
  my $rKey=(shift || return 0);

  foreach ( split(/\\&/, $env)) {
     my ($key, $val) = split(/=/,$_,2);
     if($rKey eq $key) {
       ($key ne "lfn") and return $val;
       my $lfn = $val;
       return descapeSEnvDelimiter($lfn);
     }
  }
  return 0;
}



sub deserializeSignedEnvelopes{
  (@_) > 0 or return ();
  my @envelopes = ();
  foreach (@_) {
    my $env = deserializeSignedEnvelope($_);
    (scalar(keys(%$env)) > 0) or next;
    push @envelopes, $env;
  }
  return @envelopes;
}

sub deserializeSignedEnvelope{
  my $env=(shift || return {});
  my $envelope = {};

  foreach ( split(/\\&/, $env)) {
     my ($key, $val) = split(/=/,$_,2);
     $envelope->{$key} = $val;
  }
  $envelope->{hashord} and $envelope->{signature} or return {};
  my @signedKeys= split('-', $envelope->{hashord});
  my @signedElements = ();
  foreach (@signedKeys) {
     push @signedElements, $_."=".($envelope->{$_} || 0);
  }
  push @signedElements, "hashord=".$envelope->{hashord};
  push @signedElements, "signature=".($envelope->{signature} || 0);
  $envelope->{signedEnvelope} = join('\&',@signedElements);

  $envelope->{lfn} = descapeSEnvDelimiter(($envelope->{lfn}|| 0));

  return $envelope;
}

sub escapeSEnvDelimiter{
  my $lfn=shift;
  $lfn =~ s/&/-~-/g;
  $lfn =~ s/=/-~~-/g;
  return $lfn;
}

sub descapeSEnvDelimiter{
  my $lfn=shift;
  $lfn =~ s/-~-/&/g;
  $lfn =~ s/-~~-/=/g;
  return $lfn;
}



sub getDebugLevelFromParameters{
  my $back=0;
  my @rlist=();
  foreach (@_){
     ($_ =~ /-debug=([0-9])/) and $back = $1 or push @rlist, $_;
  }
  return ($back, \@rlist);
}





return 1;

# Utility Functions for AliEn
# AJP 07/2004
# ----------------------------------------------------------

package AliEn::Util;

use strict;

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
  return "/proc/$user/$ID";
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
  return ['ASSIGNED','DONE','ERROR_A','ERROR_I','ERROR_E','ERROR_IB','ERROR_M','ERROR_R','ERROR_S','ERROR_SPLT', 'ERROR_SV','ERROR_V','ERROR_VN','EXPIRED','FAILED','FORCEMERGE','IDLE','INSERTING','INTERACTIV','KILLED','MERGING','QUEUED','RUNNING','SAVING','SAVED', 'SPLIT','SPLITTING','STARTED','WAITING','ZOMBIE'];
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
		'ERROR_A' => -1, 'ERROR_I' => -2, 'ERROR_E' => -3, 'ERROR_IB' => -4, 
		'ERROR_M' => -5, 'ERROR_R' => -6, 'ERROR_S' => -7, 'ERROR_SPLT' => -8, 
		'ERROR_SV' => -9, 'ERROR_V' => -10, 'ERROR_VN' => -11, 'EXPIRED' => -12,
		'FAILED' => -13, 'KILLED' => -14, 'ZOMBIE' => -15};

# convert a job status to a number to be used in MonaLisa
sub statusForML {
  my $stat = shift;
  #print "statusForML ($stat) => $status->{$stat}\n";
  return $ml_status->{$stat} or 0;
}


my $ml_transferStatus={'INSERTING' => 1,
		       'WAITING' => 2, 
		       'ASSIGNED' => 3,
		       'LOCAL COPY' => 4,
		       'TRANSFERING' => 5, 
		       'CLEANING' => 6,
		       'DONE' => 7, 
		       'FAILED' => -1,
		       'KILLED' => -2,
		      };

sub transferStatusForML {
  my $stat=shift;
  return $ml_transferStatus->{$stat} or 0;
  
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
  return $ja_status->{$stat} or 0;
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
  $self->{CACHE}->{$name}={expired=>$date+6000, timeChecked=>$date,
			   value=>$value};
  $self->debug(1,"Setting the cache of $name = $value at $date");
  return 1;
}
sub returnCacheValue {
  my $self=shift;
  my $name=shift;
  $self->info("Checking if we can return a value from the cache ($name)");
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
  $self->info("Returning the value from the cache ($self->{CACHE}->{$name}->{value})");
  return $self->{CACHE}->{$name}->{value};

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

return 1;

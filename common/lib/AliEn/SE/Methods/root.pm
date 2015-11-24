package AliEn::SE::Methods::root;

use AliEn::SE::Methods::Basic;
use AliEn::Logger::LogObject;

use AliEn::Util;
use strict;
use vars qw( @ISA $DEBUG);
@ISA = ("AliEn::SE::Methods::Basic");

push @ISA, 'AliEn::Logger::LogObject';

use IPC::Open2;

use strict;

sub initialize {
  my $self = shift;
  $self->{SILENT} = 1;
  $self->{CLASS}  = ref $self;
  $self->debug(1, "Let's see if xrdcpapmon is in the path");

  $self->{XRDCP} = "xrdcp";
  if (open(FILE, "which xrdcpapmon 2>&1|")) {
    my $input = join("", <FILE>);
    if (close FILE) {
      $self->debug(1, "Using $input");
      $self->{XRDCP} = "xrdcpapmon";
    }
  }

  $self->{XRD}         = "xrd";
  $self->{XRD_OPTIONS} = "-DITransactionTimeout 300 -DIFirstConnectMaxCnt 3 -DIReadCacheSize 0 ";
  return $self;
}

sub _execute {
  my $self    = shift;
  my $command = shift;

  $self->{LOGGER}->{LOG_OBJECTS}->{ $self->{CLASS} }
    or $command .= "> /dev/null 2>&1";

  $self->debug(1, "Doing $command");
  return system($command);
}

sub get {
  my $self = shift;

  my $xrddebug = "";
  $self->{DEBUG}
    and $self->{DEBUG} > 2
    and $xrddebug = " -d " . ($self->{DEBUG} - 2);

  my $command;

  my $pfn = ($self->{XURL} || $self->{PFN});
  $pfn =~ s/#.*$//;

  if ($self->{OLDENVELOPE}) {
    $command =
"$self->{XRDCP} $xrddebug $self->{XRD_OPTIONS} $self->{PROXY}$pfn $self->{LOCALFILE} -OS\\\&authz=\"$self->{OLDENVELOPE}\""
      ;
  } elsif ($self->{ENVELOPE}) {
    $command =
"$self->{XRDCP} $xrddebug $self->{XRD_OPTIONS} $self->{PROXY}$pfn $self->{LOCALFILE} -OS\\\&authz=\"$self->{ENVELOPE}\"";
    # Isn't the following branch old and useless ? Not sure !
  } else {
    $self->{PARSED}->{PATH} =~ s{^//}{/};
    my $p = $self->{PARSED}->{PATH};
    $p =~ s/#.*$//;
    $command =
"$self->{XRDCP} -d 3 $self->{XRD_OPTIONS} $self->{PROXY}root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$p $self->{LOCALFILE} ";
    $self->info(
"WARNING: AliEn root.pm did receive neither an old nor a new envelope for this call! Trying call: $command "
    );       
  }

  $self->debug(4, "CALLING WITH: $command");
  
  
  (-f $self->{LOCALFILE}) and $self->info("DELETING THE LOCALFILE") and unlink $self->{LOCALFILE};
  my $output = `$command 2>&1 ; echo "ALIEN_XRD_SUBCALL_RETURN_VALUE=\$? "`;
  $output
    or $self->info(
"ERROR Calling -- $command -- There was no returned output or it could not been captured.",
    1
    ) and return;

  $output =~ s/\s+$//;
  $output =~ /ALIEN_XRD_SUBCALL_RETURN_VALUE\=([-]*\d+)$/;
  my $com_exit_value = $1;

  $self->debug(2, "Exit code: $com_exit_value, Returned output: $output");

  (($com_exit_value eq 0) and (-f $self->{LOCALFILE}))
    or $self->info(
"ERROR: Getting the file with -- $command -- \n didn't work! Exit code: $com_exit_value, Returned output: $output",
    1
    ) and return;

  $self->debug(1, "YUUHUUUUU!!\n");
  return $self->{LOCALFILE};
}

sub put {
  my $self = shift;

  my $xrddebug = "";
  $self->{DEBUG}
    and $self->{DEBUG} > 2
    and $xrddebug = " -d " . ($self->{DEBUG} - 2);

  $self->debug(1,
"Trying to put the file $self->{PARSED}->{ORIG_PFN} (from $self->{LOCALFILE})"
  );
  $self->{PARSED}->{PATH} =~ s{^//}{/};

  my $sizetag = "";
  $self->{SIZE} and $sizetag = "?eos.bookingsize=$self->{SIZE}";

  my $command =
"$self->{XRDCP} $xrddebug $self->{XRD_OPTIONS} -np -v $self->{LOCALFILE} -f -P ";

  if ($ENV{ALIEN_XRDCP_ENVELOPE}) {
    $self->debug(1, "PUTTING THE SECURITY ENVELOPE IN THE XRDCP");
    $command .=
      " $ENV{ALIEN_XRDCP_URL}$sizetag -OD\\\&authz=\"$ENV{ALIEN_XRDCP_ENVELOPE}\"";
    $self->debug(1, "The envelope is $ENV{ALIEN_XRDCP_ENVELOPE}");
  } elsif ($ENV{ALIEN_XRDCP_SIGNED_ENVELOPE}){
    $command.=" $ENV{ALIEN_XRDCP_URL}$sizetag -OD\\\&authz=\"$ENV{ALIEN_XRDCP_SIGNED_ENVELOPE}\"";
    $self->debug(1,"The envelope is $ENV{ALIEN_XRDCP_SIGNED_ENVELOPE}");
  } else {
    $command.=" $self->{PROXY}root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}$sizetag";
    $self->info("WARNING: AliEn root.pm did receive neither an old nor a new envelope for this call! Trying call: $command "); 
  }
  
    $self->debug(1, "The command is $command");
    my $output = `$command  2>&1 ; echo "ALIEN_XRD_SUBCALL_RETURN_VALUE=\$?"`;
    $output or $self->info("ERROR Calling -- $command -- There was no returned output or it could not been captured.",1) and return;

    $output =~ s/\s+$//;
    $output =~ /ALIEN_XRD_SUBCALL_RETURN_VALUE\=([-]*\d+)$/;
    my $com_exit_value = $1;

    $self->debug(2, "Exit code: $com_exit_value, Returned output: $output");    
    
    if($com_exit_value eq 0) {    	
        my $size=-s $self->{LOCALFILE};

        $self->debug(2, "YUHUUU!! File was properly uploaded, now let's double check destination file size again ...");
        my $xrdstat = $self->xrdstat();
        ($xrdstat eq $size) or $self->info("WARNING: xrd stat not successful, waiting 6s...") and sleep(6) and $xrdstat = $self->xrdstat();
        ($xrdstat eq $size) or $self->info("WARNING: xrd stat not successful, waiting 9s...") and sleep(9) and $xrdstat = $self->xrdstat();
        ($xrdstat eq $size) or $self->info("WARNING: xrd stat not successful, waiting 30s...") and sleep(30) and $xrdstat = $self->xrdstat();
  
  		( ($xrdstat eq $size) and $self->debug(1, "EXCELLENT! Double checking file size on destination SE was successfully.") )
          or $self->info("ERROR: Double checking the file size on the SE with xrd stat showed unequal file sizes!",1) and return;
          $self->debug(2,"Double check file size value from xrd stat: $1");
  
        return "$self->{PROXY}root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
    }

  $self->info("Exit code not equal to zero. Something went wrong with xrdcp!! \n We called: $command \n Exit code: $com_exit_value, Returned output: $output",1);
  return;
}

sub xrdstat {
  my $self = shift;

  my $xrddebug = "";
  $self->{DEBUG}
    and $self->{DEBUG} > 2
    and $xrddebug = " -d " . ($self->{DEBUG} - 2);

  my $vercommand =
"$self->{XRD} $self->{PARSED}->{HOST}:$self->{PARSED}->{PORT} stat $self->{PARSED}->{PATH}";
  my $doxrcheck = `$vercommand`
    or $self->info(
"WARNING: xrd stat to double check file size after successful write was not possible!",
    1
    );
    
  $doxrcheck =~ /Size:\ (\d+)/;

  return $1;

}

sub _timeout {
  alarm(0);
  print "Timeout!!\n";
  die;
}

sub remove {
  my $self = shift;
  $self->debug(1, "Trying to remove the file $self->{PARSED}->{ORIG_PFN}");
  print "We are in the remove\n";

#  missing utilization of $ENV{ALIEN_XRDCP_TURL}  + $ENV{ALIEN_XRDCP_SIGNED_ENVELOPE}   / $ENV{ALIEN_XRDCP_ENVELOPE} !!!!!!

  #  open(FILE, "| xrd $self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}
  my $pid =
    open2(*Reader, *Writer,
    "xrd $self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}")
    or $self->info("Error calling xrd!") && return;
  print "Open\n";
  print Writer "rm $self->{PARSED}->{PATH}\n";
  print "Just wrote\n";
  my $error = close Writer;
  print "Reading\n";
  my $got      = "";
  my $oldAlarm = $SIG{ALRM};
  $SIG{ALRM} = \&_timeout;
  eval {
    alarm(5);

    while (my $l = <Reader>) {
      print "Hello '$l'\n";
      $got .= "$l";
      $l =~ /^\s*$/         and last;
      $l =~ /^\s*root:\/\// and last;
    }
  };
  my $error3 = $@;
  alarm(0);
  $oldAlarm and $SIG{ALRM} = $oldAlarm;
  print "read (with error $error3)\n";
  my $error2 = close Reader;
  print "Hello $error and $got and ($error2)\n";

#  my $command="xrm root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
#  my $error=$self->_execute($command);#
#
#  ($error<0) and return;
  $self->debug(1, "YUUHUUUUU!!\n");
  return
"root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";

}

sub getSize {
  my $self = shift;

  $self->info("Getting the size of $self->{PARSED}->{ORIG_PFN}");
  open(FILE, " xrdstat $self->{PARSED}->{ORIG_PFN}|")
    or $self->info("Error doing xrdstat")
    and return;
  my $buffer = join("", <FILE>);
  close FILE;
  $self->debug(1, "Got $buffer");
  $buffer =~ /size=(\d+) /
    or $self->info("There is no line with the size in: $buffer")
    and return;
  $self->info("The size is $1");
  return $1;
}

sub getStat {
  my $self = shift;

  $self->info("Getting the stat of $self->{PARSED}->{ORIG_PFN}");
  open(FILE, " xrdstat $self->{PARSED}->{ORIG_PFN}|")
    or $self->info("Error doing xrdstat")
    and return;
  my $buffer = join("", <FILE>);
  close FILE;
  $self->debug(1, "Got $buffer");
  return $buffer;
}

sub stage {
  my $self = shift;
  my $pfn  = shift;
  $self->info("WE HAVE TO STAGE AN XROOTD FILE ($pfn)!!");
  system("xrdstage", $pfn);
  return 1;
}

sub isStaged {
  my $self = shift;
  my $pfn  = shift;
  $self->info("Checking if the file is in the xrootd cache");
  system("xrdisonline", $pfn);
  return 1;
}
return 1;

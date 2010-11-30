package AliEn::SE::Methods::root;

use AliEn::SE::Methods::Basic;
use AliEn::Logger::LogObject;

use strict;
use vars qw( @ISA $DEBUG);
@ISA = ("AliEn::SE::Methods::Basic");

push @ISA, 'AliEn::Logger::LogObject';

use IPC::Open2;

use strict;

sub initialize {
  my $self = shift;
  $self->{SILENT}=1;
  $self->{CLASS}= ref $self;
  $self->debug(1, "Let's see if xrdcpapmon is in the path");

  $self->{XRDCP}="xrdcp";
  if (open (FILE, "which xrdcpapmon 2>&1|")){
    my $input=join("",<FILE>);
    if (close FILE){
      $self->debug(1, "Using $input");
      $self->{XRDCP}="xrdcpapmon";
    }
  }

  $self->{XRD} = "xrd";
  
  return $self;
}
sub _execute {
  my $self=shift;
  my $command=shift;

  $self->{LOGGER}->{LOG_OBJECTS}->{$self->{CLASS}}
    or $command.="> /dev/null 2>&1";

  $self->debug(1, "Doing $command");
  return system ($command);
}


sub get {
  my $self = shift;

  my $xrddebug = "";
  $self->{DEBUG} and $self->{DEBUG} > 2 and $xrddebug = " -d ".($self->{DEBUG}-2);

  my $command;

  if ($ENV{ALIEN_XRDCP_ENVELOPE}){
    my $p=$ENV{ALIEN_XRDCP_URL};
    $p=~ s/#.*$//;
    $command="$self->{XRDCP} $xrddebug -DIFirstConnectMaxCnt 6 $p $self->{LOCALFILE} -OS\\\&authz=\"$ENV{ALIEN_XRDCP_ENVELOPE}\"";
    $self->debug(1, "The envelope is $ENV{ALIEN_XRDCP_ENVELOPE}");
    $self->debug(1,"Trying to get the file $self->{PARSED}->{ORIG_PFN} (to $self->{LOCALFILE})");
  # Isn't the following branch old and useless ? Not sure !
  } else {
    $self->{PARSED}->{PATH}=~ s{^//}{/};
    my $p= $self->{PARSED}->{PATH};
    $p =~ s/#.*$//;
    $command="$self->{XRDCP} -d 3 -DIFirstConnectMaxCnt 6 root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$p $self->{LOCALFILE} ";
    $self->debug(1,"Trying to get the file $self->{PARSED}->{ORIG_PFN} (to $self->{LOCALFILE})");
  }
  
  my $output = `$command 2>&1 ; echo "ALIEN_XRD_SUBCALL_RETURN_VALUE=\$?"` or $self->info("ERROR: Not possible to call $self->{XRDCP}!",1) and return;
  $output =~ s/\s+$//;
  $output =~ /ALIEN_XRD_SUBCALL_RETURN_VALUE\=([-]*\d+)$/;
  my $com_exit_value = $1;
  
  $self->debug(2, "Exit code: $com_exit_value, Returned output: $output");

  ( ($com_exit_value eq 0) and (-f $self->{LOCALFILE}) ) 
      or $self->info("ERROR: Getting the file with xrdcp didn't work! Exit code: $com_exit_value, Returned output: $output",1) and return;

  $self->debug(1,"YUUHUUUUU!!\n");
  return $self->{LOCALFILE};
}

sub put {
  my $self=shift;
 
   my $xrddebug = "";
  $self->{DEBUG} and $self->{DEBUG} > 2 and $xrddebug = " -d ".($self->{DEBUG}-2);


  $self->debug(1,"Trying to put the file $self->{PARSED}->{ORIG_PFN} (from $self->{LOCALFILE})");
  $self->{PARSED}->{PATH}=~ s{^//}{/};

  my $command="$self->{XRDCP} $xrddebug -DIFirstConnectMaxCnt 6 -np -v $self->{LOCALFILE} -f ";


  if ($ENV{ALIEN_XRDCP_ENVELOPE}){
    $self->debug(1,"PUTTING THE SECURITY ENVELOPE IN THE XRDCP");
    $command.=" $ENV{ALIEN_XRDCP_URL} -OD\\\&authz=\"$ENV{ALIEN_XRDCP_ENVELOPE}\"";
    $self->debug(1,"The envelope is $ENV{ALIEN_XRDCP_ENVELOPE}");

  } else {
    $command.=" root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
  }
  $self->debug(1,"The command is $command");
  my $output = `$command  2>&1 ; echo "ALIEN_XRD_SUBCALL_RETURN_VALUE=\$?"` or $self->info("Error: xrdcp is not in the path",1) and return;
  $output =~ s/\s+$//; 
  $output =~ /ALIEN_XRD_SUBCALL_RETURN_VALUE\=([-]*\d+)$/;
  my $com_exit_value = $1;

  $self->debug(2, "Exit code: $com_exit_value, Returned output: $output");
  if($com_exit_value eq 0) {
      $output=~ /Data Copied \[bytes\]\s*:\s*(\d+)/;
      $self->info("Transfered  $1  bytes");
      my $size=-s $self->{LOCALFILE};
      if ($size eq $1){
         $self->info("YUHUUU!! File was properly uploaded, now let's double check destination file size again ...");
         my $xrdstat = $self->xrdstat();
         ($xrdstat eq $size) or $self->info("WARNING: xrd stat not successful, waiting 6s...") and sleep(6) and $xrdstat = $self->xrdstat();
         ($xrdstat eq $size) or $self->info("WARNING: xrd stat not successful, waiting 9s...") and sleep(9) and $xrdstat = $self->xrdstat();
         ($xrdstat eq $size) or $self->info("WARNING: xrd stat not successful, waiting 30s...") and sleep(30) and $xrdstat = $self->xrdstat();


         ( ($xrdstat eq $size) and $self->info("EXCELLENT! Double checking file size on destination SE was successfully.") )
             or $self->info("ERROR: Double checking the file size on the SE with xrd stat showed unequal file sizes!",1) and return;
         $self->debug(2,"Double check file size value from xrd stat: $1");

      }
      return "root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
  }
  $self->info("Exit code not equal to zero. Something went wrong with xrdcp!! Exit code: $com_exit_value, Returned output: $output",1);
  return;
}

sub xrdstat {
  my $self=shift;

   my $xrddebug = "";
  $self->{DEBUG} and $self->{DEBUG} > 2 and $xrddebug = " -d ".($self->{DEBUG}-2);


   my $vercommand = "$self->{XRD} $self->{PARSED}->{HOST}:$self->{PARSED}->{PORT} stat $self->{PARSED}->{PATH}";
   my $doxrcheck = `$vercommand` or $self->info("WARNING: xrd stat to double check file size after successful write was not possible!",1);
   $doxrcheck =~ /Size:\ (\d+)/;

  return $1;

}

sub _timeout {
  alarm(0);
  print "Timeout!!\n";
  die;
}
sub remove {
  my $self=shift;
  $self->debug(1,"Trying to remove the file $self->{PARSED}->{ORIG_PFN}");
  print "We are in the remove\n";
#  open(FILE, "| xrd $self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}
  my $pid = open2(*Reader, *Writer, "xrd $self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}" )
    or $self->info("Error calling xrd!") && return;
  print "Open\n";
  print Writer "rm $self->{PARSED}->{PATH}\n";
  print "Just wrote\n";
  my $error=close Writer;
  print "Reading\n";
  my $got="";
  my $oldAlarm=$SIG{ALRM};
  $SIG{ALRM}=\&_timeout;
  eval {
    alarm(5);
    while(my $l=<Reader>){
      print "Hello '$l'\n";
      $got.="$l";
      $l=~ /^\s*$/ and last;
      $l=~ /^\s*root:\/\// and last;
    }
  };
  my $error3=$@;
  alarm(0);
  $oldAlarm and $SIG{ALRM}=$oldAlarm;
  print "read (with error $error3)\n";
  my $error2=close Reader;
  print "Hello $error and $got and ($error2)\n";
#  my $command="xrm root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
#  my $error=$self->_execute($command);#
#
#  ($error<0) and return;
  $self->debug(1,"YUUHUUUUU!!\n");
  return "root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";

}

sub getSize {
  my $self=shift;

  $self->info("Getting the size of $self->{PARSED}->{ORIG_PFN}");
  open (FILE, " xrdstat $self->{PARSED}->{ORIG_PFN}|") or 
    $self->info("Error doing xrdstat") and return;
  my $buffer=join("", <FILE>);
  close FILE;
  $self->debug(1,"Got $buffer");
  $buffer=~ /size=(\d+) / or $self->info("There is no line with the size in: $buffer") and return;
  $self->info("The size is $1");
  return $1;
}

sub getStat {
  my $self=shift;

  $self->info("Getting the stat of $self->{PARSED}->{ORIG_PFN}");
  open (FILE, " xrdstat $self->{PARSED}->{ORIG_PFN}|") or
    $self->info("Error doing xrdstat") and return;
  my $buffer=join("", <FILE>);
  close FILE;
  $self->debug(1,"Got $buffer");
  return $buffer;
}



sub stage {
  my $self=shift;
  my $pfn=shift;
  $self->info("WE HAVE TO STAGE AN XROOTD FILE ($pfn)!!");
  system("xrdstage", $pfn);
  return 1;
}

sub isStaged{
  my $self=shift;
  my $pfn=shift;
  $self->info("Checking if the file is in the xrootd cache");
  system("xrdisonline", $pfn);
  return 1;
}
return 1;

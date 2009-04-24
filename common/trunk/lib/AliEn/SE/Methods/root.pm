package AliEn::SE::Methods::root;

use AliEn::SE::Methods::Basic;

use strict;
use vars qw( @ISA $DEBUG);
@ISA = ("AliEn::SE::Methods::Basic");

$DEBUG=0;
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

  $self->debug(1,"Trying to get the file $self->{PARSED}->{ORIG_PFN} (to $self->{LOCALFILE})");
  $self->{PARSED}->{PATH}=~ s{^//}{/};
  my $p= $self->{PARSED}->{PATH};
  $p =~ s/#.*$//;
  my $command="$self->{XRDCP} -DIFirstConnectMaxCnt 6  root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$p $self->{LOCALFILE} ";

  if ($ENV{ALIEN_XRDCP_ENVELOPE}){
    my $p=$ENV{ALIEN_XRDCP_URL};
    $p=~ s/#.*$//;
    $command="$self->{XRDCP} -DIFirstConnectMaxCnt 6 $p $self->{LOCALFILE} -OS\\\&authz=\"$ENV{ALIEN_XRDCP_ENVELOPE}\"";
    $self->debug(1, "The envelope is $ENV{ALIEN_XRDCP_ENVELOPE}");
  }
  

# At the moment, xrdcp doesn't return properly. Let's check if the file exists
  $self->_execute($command);

  (-f $self->{LOCALFILE}) or return;

  $self->debug(1,"YUUHUUUUU!!\n");
  return $self->{LOCALFILE};
}

sub put {
  my $self=shift;
  $self->debug(1,"Trying to put the file $self->{PARSED}->{ORIG_PFN} (from $self->{LOCALFILE})");

  $self->{PARSED}->{PATH}=~ s{^//}{/};

  $self->debug(1,"PUTTING THE SECURITY ENVELOPE IN THE XRDCP");

  my $command="$self->{XRDCP} -DIFirstConnectMaxCnt 6 -np -v $self->{LOCALFILE} ";

  if ($ENV{ALIEN_XRDCP_ENVELOPE}){
    $command.="$ENV{ALIEN_XRDCP_URL} -OD\\\&authz=\"$ENV{ALIEN_XRDCP_ENVELOPE}\"";
    $self->debug(1,"The envelope is $ENV{ALIEN_XRDCP_ENVELOPE}");
  } else {
    $command.="root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
  }
#  my $error = $self->_execute($command);
  $self->debug(1,"The command is $command");
  open (OUTPUT, "$command  2>&1 |") or $self->info("Error: xrdcp is not in the path") and return;
  my @output=<OUTPUT>;
  close OUTPUT;
  $self->debug(2, "Got the output: @output");
  my ($line)=grep (/Data Copied \[bytes\]\s*:\s*(\d+)/, @output);
  if ($line){
    $line=~ /Data Copied \[bytes\]\s*:\s*(\d+)/;
    $self->info("Transfered  $1  bytes");
    my $size=-s $self->{LOCALFILE};
    if ($size eq $1){
      $self->info("YUHUUU!!");
      return "root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
    } 
    $self->info("The file has not been completely transfered");
    return;
  }

#  $error and return;
  $self->info("Something went wrong with xrdcp!!\n @output\n");
  return;
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
    or $self->info("Error calling xrd!") and return;
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

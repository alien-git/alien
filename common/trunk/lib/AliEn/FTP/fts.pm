package AliEn::FTP::fts;

use strict;

use strict;
use vars qw(@ISA $DEBUG);
use AliEn::FTP;
@ISA = ( "AliEn::FTP" );

use Net::LDAP;
use AliEn::Util;

sub initialize {
  my $self=shift;
  my $options = shift;
  
  $self->info("Contacting the BDII");
  my $BDII=$ENV{LCG_GFAL_INFOSYS} || 'sc3-bdii.cern.ch:2170';

  $self->{BDII_BASE}="mds-vo-name=local,o=grid";
  $self->{BDII}=    Net::LDAP->new( $BDII) or
    print STDERR "Error contacting ldap at $BDII: $@" and return;
  
  $self->{BDII}->bind or print STDERR "Error binding to LDAP" and return;

  my $command = `which glite-transfer-submit 2> /dev/null`;
  ($command && !$?) or printf STDERR "Error: No glite-transfer-submit command found in your path.\n" and exit 5;
  chomp $command;
  $command or $self->info("Error: could not find glite-transfer-submit") and return;
  $self->{COMMAND}=$command;

  $self->{FTS_ENDPOINT}={};
  $self->{FTS_ENDPOINT}="https://fts-t0-export.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer";

  # Setup the properties for monitoring. These whould come from FTD
  for my $opt ("MONITOR", "FTD_TRANSFER_ID", "SITE_NAME", "FTD_FULLNAME"){
    $self->{$opt} = $options->{$opt};
  }
  return $self;
}



sub copy {
  my $self=shift;
  my $source=shift;
  my $target=shift;
  my $line=shift;

  $self->info("Ready to copy $source->{turl} into $target->{turl} with fts");

  my ($protocol, $se, $sourceHost, $targetHost)=split(',', $line);


  $sourceHost =~ s/^host=(.*)/$1/i or $self->info("Error getting the source host from $line") and return;

  $targetHost =~ s/^host=(.*)/$1/i or $self->info("Error getting the target host from $line") and return;

#  my $from=$source->{pfn};
#  my $to=$target->{pfn};
  my $from=$source->{turl};
  my $to=$target->{turl};

  my @splitturl = split (/\/\//, $source->{turl},3);
  $splitturl[2] and  $from=$splitturl[2];

  @splitturl = split (/\/\//, $target->{turl},3);
  $splitturl[2] and  $to=$splitturl[2];


  print "READY TO TRANSFER from $sourceHost to $targetHost using $self->{FTS_ENDPOINT}\n";

  my $transfer="$self->{COMMAND} -o --verbose -s $self->{FTS_ENDPOINT} srm://$sourceHost$from srm://$targetHost$to";
  $self->info("Ready to do the transfer: $transfer");

  $self->prepareEnvironment();

  my $done=0;

  open (FILE, "$transfer|") or $self->info("Error doing the command!!", 1) and $self->restoreEnvironemnt() and return ;
  my $id=join("", <FILE>);
  close FILE or $self->info("Error executing $self->{COMMAND}", 1) and $self->restoreEnvironment() and return ;
  $self->restoreEnvironment();
  chomp $id;
  $id or $self->info("Error getting the transferId",1) and return;
  $self->info("Transfer issued!! $id");
  return (2, $id);
#  my $retry=10;
#  while(1) {
#    sleep (40);
#    $self->info("Checking if the transfer $id has finished");
#    my $status=$self->checkStatusTransfer($ftsEndpoint, $id)
#      or last;
#    if ($status<0){
#      $self->info("Something went wrong");
#      $retry+=$status;
#      if ($retry <0){
#	$self->info("Giving up");
#	return ;
#      }	
#    }
#  }
#
#  $self->info("So far, so good");
#
#  return 1;
}


sub prepareEnvironment{
  my $self=shift;
  $self->{OLD_ENV}={};
  $self->debug(2, "Removing the environment variables for the FTS call");
  foreach ("X509_USER_KEY", "X509_USER_CERT"){
    $ENV{$_} or next;
    $self->{OLD_ENV}->{$_}=$ENV{$_};
    delete $ENV{$_};
  }
#  print "The LD_LIBRARY_PATH is $ENV{LD_LIBRARY_PATH}
#ROOT $ENV{ALIEN_ROOT}\n";
  $self->{OLD_ENV}->{LD_LIBRARY_PATH}=$ENV{LD_LIBRARY_PATH};
  my $d=$ENV{ALIEN_ROOT}."[^:]";
  $ENV{LD_LIBRARY_PATH}=~ s/$d*\://g;
#  print "NOW $ENV{LD_LIBRARY_PATH}\n";
  return 1;
}

sub restoreEnvironment{
  my $self=shift;
  $self->{OLD_ENV} or return 1;
  $self->debug(2, "Restoring the environment for alien");
  foreach (keys %{$self->{OLD_ENV}}) {
    $ENV{$_}=$self->{OLD_ENV}->{$_};
  }
  return 1;
}
sub checkStatusTransfer {
  my $self=shift;
  my $id=shift;
  my $done=0;


  $self->prepareEnvironment();
  my @status=AliEn::Util::_system("glite-transfer-status -s $self->{FTS_ENDPOINT} $id");
  $self->restoreEnvironment();
  @status or $self->info("Error checking the status of the transfer $id : $!",2) and return -1;
  my $status=join("", @status);
  $DEBUG and print "$status\n";
  chomp $status;

  $self->sendTransferStatus($id, $status);
  if ($status =~ /(fail)|(Canceled)|(FinsihedDirty)/i){

    $self->prepareEnvironment();
    my @output=AliEn::Util::_system("glite-transfer-status -l --verbose -s $self->{FTS_ENDPOINT} $id");
    my $fileStatus=join("", @output);
    $self->restoreEnvironment();
    my $reason="";
    $fileStatus=~ /^\s+Reason: (.*)/m and $reason=$1;
    $self->info("The FTS transfer $id failed ($reason)",2);
    return -11;
  }elsif($status =~ /(active)|(submitted)|(pending)|(ready)/i){
    $self->info("Transfer still waiting");
    return 1
  }elsif($status =~ /(done)|(Finished)/i){
    $self->info("Transfer done!!!");
    return 0;
  }
  $self->info("Don't know what the status $status means (transfer $id)",2);

  return -1;
}

sub getFTSEndpoint {
  my $self=shift;
  my $site=shift;
  my $retry=5;
  my $sleep=1;

  if ($ENV{ALIEN_FTS_ENDPOINT}){
    $self->info("The environment variable ALIEN_FTS_ENDPOINT is set ('$ENV{ALIEN_FTS_ENDPOINT}'). Using it as the endpoint");
    return $ENV{ALIEN_FTS_ENDPOINT};
  }
 $site or return;
  while ($retry){
    my $cache=AliEn::Util::returnFileCacheValue($self, "fts-$site");
    if ($cache) {
      $self->info(" $$ Returning the value from the cache ($cache)");
      return $cache;
    }
    $retry--;
    $self->info("Getting the FTSendpoint of $site from the BDII");
    my $mesg=$self->{BDII}->search( base=>$self->{BDII_BASE},
				    filter=>"(&(GlueServiceType=org.glite.FileTransfer)(GlueForeignKey=GlueSiteUniqueId*$site))"
				  );
    if (!$mesg->count){
      $sleep = $sleep*2 + int(rand(2));
      sleep ($sleep);
      my $BDII=$ENV{LCG_GFAL_INFOSYS} || 'sc3-bdii.cern.ch:2170';

      print "Error finding the FTS endpoint for $site in $BDII. Let's sleep ($sleep seconds) and try again\n";
      $self->{BDII}=    Net::LDAP->new( $BDII) or 
	$self->info("Error contacting ldap at $BDII: $@") and return;
      $self->{BDII}->bind or $self->info("Error binding to LDAP") and return;
      next;
    }
    $mesg->count>1 and print "Warning!! there are more than one fts endpoints for $site\n";

    my $value=$mesg->entry(0)->get_value("GlueServiceEndPoint");
    eval {
      for (my $i=1; $i<$mesg->count(); $i++) {
	my $v2=$mesg->entry($i)->get_value("GlueServiceEndPoint");
	print "Shall we use $v2??\n";
	$v2=~ /prod/ and print "SIPE\n" and $value=$v2;
      }
    };
    AliEn::Util::setFileCacheValue($self, "fts-$site", $value);

    return $value;
  }
  $self->info("Couldn't get the fts endpoint from $site");
  return;

}

# Send to ML the information about this FTS transfer
sub sendTransferStatus {
  my $self      = shift;
  my $ftsID     = shift;
  my $ftsStatus = shift;
  
  if($self->{MONITOR}){
    my $params = { ftsID => $ftsID, 
                   ftsStatus => $ftsStatus,
                 };
    $self->{MONITOR}->sendParameters($self->{SITE_NAME}."_FTS_".$self->{FTD_FULLNAME}, $self->{FTD_TRANSFER_ID}, $params);
  }
}

return 1;


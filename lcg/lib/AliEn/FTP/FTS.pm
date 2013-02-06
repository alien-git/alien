package AliEn::FTP::FTS;

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
#  $ENV{ALIEN_MYPROXY_PASSWORD} or 
#    $self->info("Error: the myproxy password has not been set. Please, define it in the environment variable  ALIEN_MYPROXY_PASSWORD") and return;

  # Setup the properties for monitoring. These whould come from FTD
  for my $opt ("MONITOR", "FTD_TRANSFER_ID", "SITE_NAME", "FTD_FULLNAME"){
    $self->{$opt} = $options->{$opt};
  }
  return $self;
}


# ***************************************************************
# Creates a new token randomly. Alway 32 caracters long.
# ***************************************************************
my $createPasswd = sub {
    my $token = "";
    my @Array = (
        'X', 'Q', 't', '2', '!', '^', '9', '5', '3', '4', '5', 'o',
        'r', 't', '{', ')', '}', '[', ']', 'h', '9', '|', 'm', 'n', 'b', 'v',
        'c', 'x', 'z', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', ':', 'p',
        'o', 'i', 'u', 'y', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
        'A', 'S', 'D', 'F', 'G', 'H', 'J', 'Z', 'X', 'C', 'V', 'B', 'N', 'M'
    );
    my $i;
    for ( $i = 0 ; $i < 32 ; $i++ ) {
        $token .= $Array[ rand(@Array) ];
    }
    return $token;
};

#All the instances should have the same myproxy password
my $myProxyPassword=$createPasswd->();
sub myP {
  return  $myProxyPassword;
}



sub createMyProxy{
  my $self=shift;
  my $command="myproxy-init -S -d";
  # create an Expect object by spawning another process
  my $exp = Expect->spawn($command)
  or die "Cannot spawn $command: $!\n";
  $exp->expect(5, "Your proxy is valid until") or 
    $self->info("The proxy needed a password") and return;
  $exp->send($myProxyPassword."\n");
  $exp->soft_close() and 
    $self->info("Error creating the myproxy password") and return;
  $self->debug(1, "Myproxy created");
    
  
  return 1;
}

sub dirandfile {
    my $fullname = shift;
    $fullname =~ /(.*)\/(.*)/;
    my @retval = ( $1, $2 );
    return @retval;
}

sub put {
  my $self       = shift;
  my $localfile  = shift;
  my $remotefile = shift;
  return $self->transfer($localfile, $remotefile, @_);
}

sub get {
  my $self       = shift;
  my $localfile  = shift;
  my $remotefile = shift;

  return $self->transfer($remotefile, $localfile,  @_);
}

sub transfer {
  my $self    = shift;
  my $from    = shift;
  my $to      = shift;
  my $options =shift;
  my $sourceCertificate =shift || "";
  my $fromHost=shift;
  my $toHost=shift;

  print "\n  Configuring the transfer...\nTransfering from ($fromHost) $from to ($toHost) $to and $options\n";

  my $fromSite=$self->getSite($fromHost) ;
  my $toSite=$self->getSite($toHost) ;
  $fromSite or $toSite or ( $self->info("Don't know neither source, nor destination") and return -1) ;

  my $fromftsEndpoint=$self->getFTSEndpoint($fromSite);
  my $toftsEndpoint=$self->getFTSEndpoint($toSite);
  my $ftsEndpoint;
  if ($fromftsEndpoint && $toftsEndpoint) {
    $self->info("The FTS is defined in both sites. Which one to take??");
    if ($fromSite =~ /cern/i) {
      $self->info("Taking CERN: $fromftsEndpoint");
      $ftsEndpoint=$fromftsEndpoint;
    } else {
      $self->info("Taking the destination $toSite: $toftsEndpoint");
      $ftsEndpoint=$toftsEndpoint;
    }
  }elsif( $fromftsEndpoint) {
    $self->info("Using endpoint of $fromSite: $fromftsEndpoint");
    $ftsEndpoint=$fromftsEndpoint;
  }elsif($toftsEndpoint) {
    $self->info("Using endpoint of $toSite: $toftsEndpoint");
    $ftsEndpoint=$toftsEndpoint;
  }else {
    $self->info("Couldn't get the fts endpoint of $fromSite or from $toSite", 1);
    return -1;
  }


  my $transfer="$self->{COMMAND} -o --verbose -s $ftsEndpoint srm://$fromHost$from srm://$toHost/$to";
  $self->info("Ready to do the transfer: $transfer");
  $self->prepareEnvironment();

  my $done=0;

  open (FILE, "$transfer|") or $self->info("Error doing the command!!", 1) and $self->restoreEnvironemnt() and return -1;
  my $id=join("", <FILE>);
  close FILE or $self->info("Error executing $self->{COMMAND}", 1) and $self->restoreEnvironment() and return -1;
  $self->restoreEnvironment();
  chomp $id;
  $id or $self->info("Error getting the transferId",1) and return -1;
  my $retry=10;
  while(1) {
    sleep (40);
    $self->info("Checking if the transfer $id has finished");
    my $status=$self->checkStatusTransfer($ftsEndpoint, $id)
      or last;
    if ($status<0){
      $self->info("Something went wrong");
      $retry+=$status;
      if ($retry <0){
	$self->info("Giving up");
	return -1;
      }	
    }
  }

  $self->info("So far, so good");
  return 0;
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
  my $fts=shift;
  my $id=shift;
  my $done=0;

  $self->prepareEnvironment();
  my @status=AliEn::Util::_system("glite-transfer-status -s $fts $id");
  $self->restoreEnvironment();
  @status or $self->info("Error checking the status of the transfer $id : $!",2) and return -1;
  my $status=join("", @status);
  $DEBUG and print "$status\n";
  chomp $status;

  $self->sendTransferStatus($id, $status);
  if ($status =~ /(fail)|(Canceled)|(FinsihedDirty)/i){

    $self->prepareEnvironment();
    my @output=AliEn::Util::_system("glite-transfer-status -l --verbose -s $fts $id");
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

sub getSite {
  my $self=shift;
  my $host=shift;
  $host =~ s/:\d+$//;

  $self->info("Searching for the site of $host");

  my $mesg = $self->{BDII}->search( base   => $self->{BDII_BASE},
				    filter => "(&(objectClass=GlueSE)(GlueSEUniqueID=$host))"
#				    filter => "(GlueSEUniqueID=$host)"
				  );
  
  my   $total = $mesg->count;

  $total or $self->info("Error: Don't know the site of $host", 1) and return;

  $total >1 and $self->info("Warning!! the se $host is in more than one site");
  my $entry1=$mesg->entry(0);
  my @site=$entry1->get_value("GlueForeignKey");
  my $site;
  foreach my $entry (@site) {
    $entry=~ s/^GlueSite(Unique)?ID=// or next;
    $site=$entry;
    $self->info("Site $entry");
  }
  $site or $self->info("Error: the entry ".$entry1->dn()." does not define GlueForeignKey=GlueSiteID=<sitename>") and return;
  $self->info("The se $host is in $site");
  return $site;
}

sub startListening {
  my $self=shift;
  $self->info("Starting the FTS server");
}

sub getURL{
  my $self=shift;
  my $file=shift;
  $self->info("Checking the fts url of $file");
  $file=~ s/^srm:/fts:/ and return $file;
  if ($file =~ s{^((castor)|(root))://([^/]*)}{}){
    my $host2;
    my $site=$4;
    $site=~ s{^[^\.]*.}{};
    
    $self->info("The file is in castor... we have to find the srm endpoint of $site");
    my $sleep=1;
    my $try=5;
    while ($try) {
      eval {
	print "Connecting to the BDII\n";
	my $BDII=$ENV{LCG_GFAL_INFOSYS} || 'sc3-bdii.cern.ch:2170';
	$self->{BDII}=    Net::LDAP->new( $BDII) or 
	  die("Error contacting ldap at $BDII: $@");
	$self->{BDII}->bind or die("Error binding to LDAP");
	print "Searching in the BDII $BDII\n";

	my $mesg=$self->{BDII}->search( base=>$self->{BDII_BASE},
					filter=>"(&(GlueSEUniqueID=*$site)(GlueSEType=srm)(GlueSchemaVersionMinor=2))"
				      );
	print "Checking the result\n";
	
	$mesg->count or print "Error finding the SRM endpoint for $site\n" and return;
	$mesg->count>1 and print "Warning!! there are more than one srm endpoints for $site\n";
	
#	$host2=$mesg->entry(0)->get_value("GlueServiceAccessPointURL");
#	$host2=~ s{^[^:]*://}{};
	$host2=$mesg->entry(0)->get_value("GlueSEUniqueID");
	my $port=$mesg->entry(0)->get_value("GlueSEPort");
	$port and $host2.=":$port";
      };
      ($@) and	$self->info("Got the error: $@");

      $host2 and last;
      $self->info("Error getting the info from the BDII... let's try to sleep and reconnect (still $try times to try)");
      $try--;
      sleep($sleep);
      $sleep=($sleep+int(rand(10)))*2;
    }
    if ($host2){
      $self->info("Returning fts://$host2$file");
      return "fts://$host2$file";
    }

  }
  $self->info("The file $file is not in srm. It can't be transfered through fts...");
  return;
}

sub testTransfer {
  my $self=shift;
  my $source=shift;
  my $target=shift;

  $source =~ s{^srm://([^/]*)(/.*$)}{$2} or $self->info("Format of $source is not correct") and return;
  my $sourceHost=$1;
  $target =~ s{^srm://([^/]*)(/.*$)}{$2} or $self->info("Format of $target is not correct") and return;
  my $targetHost=$1;
  return $self->transfer($source, $target, "", "", $sourceHost, $targetHost);
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


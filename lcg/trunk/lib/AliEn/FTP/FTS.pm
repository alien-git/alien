package AliEn::FTP::FTS;

use strict;

use strict;
use vars qw(@ISA $DEBUG);
use AliEn::FTP;
@ISA = ( "AliEn::FTP" );

use Net::LDAP;


sub initialize {
  my $self=shift;
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
  $ENV{ALIEN_MYPROXY_PASSWORD} or 
    $self->info("Error: the myproxy password has not been set. Please, define it in the environment variable  ALIEN_MYPROXY_PASSWORD") and return;

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

  my $fromSite=$self->getSite($fromHost) or return -1;
  my $toSite=$self->getSite($toHost) or return -1;
  
  my $fromftsEndpoint=$self->getFTSEndpoint($fromSite);
  my $toftsEndpoint=$self->getFTSEndpoint($toSite);

  my $ftsEndpoint;
  if ($fromftsEndpoint && $toftsEndpoint) {
    $self->info("The FTS is defined in both sites. Which one to take??");
    if ($fromSite =~ /cern/) {
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
    $self->info("Couldn't get the fts endpoint of $fromSite or from $toSite");
  }

  my $transfer="$self->{COMMAND} --verbose -s $ftsEndpoint -p \"$ENV{ALIEN_MYPROXY_PASSWORD}\" srm://$fromHost$from srm://$toHost/$to";
  $self->info("Ready to do the transfer: $transfer");

  open (FILE, "$transfer|") or $self->info("Error doing the command!!") and return -1;
  my $id=join("", <FILE>);
  close FILE or $self->info("Error executing $self->{COMMAND}") and return -1;
  chomp $id;
  $id or $self->info("Error getting the transferId") and return -1;

  while(1) {
    sleep (10);
    $self->info("Checking if the transfer has finished");
    my $status=$self->checkStatusTransfer($ftsEndpoint, $id)
      or last;
    $status<0 and $self->info("Something went wrong") and return -1;
  }

  $self->info("So far, so good");
  return 0;
}

sub checkStatusTransfer {
  my $self=shift;
  my $fts=shift;
  my $id=shift;

  open (FILE, "glite-transfer-status --verbose -s $fts $id|") or 
    $self->info("Error checking the status") and return -1;
  my $fileStatus=join ("", <FILE>);
  close FILE or $self->info("Error checking the status") and return -1;
  $DEBUG and print "$fileStatus\n";
  $fileStatus=~ /^Status:\s*(\S*)/m  or $self->info("Error getting the status of the transfer  $fts $id") and return -1;
  my $status=$1;
  if ($status =~ /fail/i){
    $self->info("The transfer failed");
    return -1;
  }elsif($status =~ /(active)|(submitted)|(pending)/i){
    $self->info("Transfer still waiting");
    return 1
  }elsif($status =~ /done/i){
    $self->info("Transfer done!!!");
    return 0;
  }
  print "Don't know what the status $status means...\n";

  return -1;
}

sub getFTSEndpoint {
  my $self=shift;
  my $site=shift;
  my $date=time;
  if (! $self->{FTS_ENDPOINT}->{$site} || 
      $self->{FTS_ENDPOINT}->{$site}->{time}<$date) {
    $self->info("Getting the FTSendpoint from the BDII");

    my $mesg=$self->{BDII}->search( base=>$self->{BDII_BASE},
				    filter=>"(&(GlueServiceType=org.glite.FileTransfer)(GlueForeignKey=GlueSiteUniqueId*$site))"
				  );
    $mesg->count or print "Error finding the FTS endpoint for $site\n" and return;
    $mesg->count>1 and print "Warning!! there are more than one fts endpoints for $site\n";

    my $value=$mesg->entry(0)->get_value("GlueServiceEndPoint");
    $self->{FTS_ENDPOINT}->{$site}={time=>$date+600,
				    value=>$value};
  }
  return $self->{FTS_ENDPOINT}->{$site}->{value};
}


sub getSite {
  my $self=shift;
  my $host=shift;
  $host =~ s/:\d+$//;

  $self->info("Searching for the site of $host");

  my $mesg = $self->{BDII}->search( base   => $self->{BDII_BASE},
				    filter => "(&(objectClass=GlueSite)(GlueForeignKey=GlueSEUniqueID*$host))"
				  );
  
  my   $total = $mesg->count;
  $total or $self->info("Error: Don't know the site of $host") and return;
  $total >1 and $self->info("Warning!! the se $host is in more than one site");
  my $entry=$mesg->entry(0);
  my $site=$entry->get_value("GlueSiteUniqueID") ||    
    $entry->get_value("GlueSiteName");
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
  $file=~ s/^srm:/fts:/ or $self->info("The file $file is not in srm. It can't be transfered through fts...") and return;
  return $file;
}

sub testTransfer {
  my $self=shift;
  my $source=shift;
  my $target=shift;

  $source =~ s{^srm://([^/]*)(/.*$)}{$1} or $self->info("Format of $source is not correct") and return;
  my $sourceHost=$1;
  $target =~ s{^srm://([^/]*)(/.*$)}{$1} or $self->info("Format of $target is not correct") and return;
  my $targetHost=$1;
  return $self->transfer($source, $target, "", "", $sourceHost, $targetHost);
}

return 1;


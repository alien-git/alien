package AliEn::FTP::FTS;

use strict;

use GLite::Data::FileTransfer;

use AliEn::Logger::LogObject;
use vars qw($VERSION @ISA);


push @ISA, 'AliEn::Logger::LogObject';


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
$ENV{MYPROXY_SERVER}="myproxy.cern.ch";
#$ENV{LCG_CATALOG_TYPE}="lfc";
#$ENV{LFC_HOST}="lfc-alice-test.cern.ch";
#$ENV{LFC_HOME}="/grid/alice/test";
#$ENV{LCG_GFAL_INFOSYS}="sc3-bdii.cern.ch:2170";
my $FTSEndpoint='https://fts-alice-test.cern.ch:8443/alice-pilot/glite-data-transfer-fts/services/FileTransfer';

sub new {
    my $proto   = shift;
    my $options = shift;
    my $class   = ref($proto) || $proto;
    my $self    = {};
    bless( $self, $class );

    $self->{DESTHOST} = $options->{HOST};

    bless $self, $class;
    $self->SUPER::new() or return;
    $self->createMyProxy() or return;
    return $self;
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

    print "\n  Configuring the transfer...\n";

    print "Endpoint:\t $FTSEndpoint\n";
    my $transfer = GLite::Data::FileTransfer->new($FTSEndpoint);
    print "Server version:\t", $transfer->getVersion(), "\n";
    print "Schema version:\t", $transfer->getSchemaVersion(), "\n";
    print "interface version:\t", $transfer->getInterfaceVersion(), "\n";

    my $params = { 'keys'	=> [ "" ],
		   'values' => [ "" ] };
    my @transferJobElements = ();
    my $timestamp=time();
    
#    my $from = $surls{$file};
#    my $to = "$toSRM/$file";
    print "From SURL:\t$from\n";
    print "To SURL:\t$to\n";
    push @transferJobElements, { transferParams => $params,
				 
				 source	      => $from,
				 dest	      => $to };
    
    
    ## Submit the transfer
    print "\n  Submitting the transfer...\n";
    print "\n  Dump the job elements...\n";
    print Dumper(@transferJobElements);
    my $job = { jobParams	=> $params,
		credential     => $myProxyPassword, 
		transferJobElements => \@transferJobElements };
    print "\n  Dump the job...\n";
    print Dumper($job);
    my $requestId = '';			
    my $start = time();
    print "\n Submit the transfer...\n";
    $requestId = $transfer->submit($job);
    $requestId or die ("Error in submitting the transfer");
    print "Request ID:\t$requestId\n" if $requestId;
    
    
    ## Now wait for the transfer to finish.
    print "\n  Waiting for the transfer to finish...\n";
    my $status = $transfer->getTransferJobStatus($requestId);
    print Dumper($status);
    my $filestatus = $transfer->getFileStatus($requestId,0,1000); 
    print Dumper($filestatus);
    while ($status->{'jobStatus'} !~ /Done|Failed|Hold/i) {
      my $now = time()-$start;
      print "Got status ".$status->{'jobStatus'}." after ".$now." seconds.\n";
      sleep 10;
      $status = $transfer->getTransferJobStatus($requestId);
      $filestatus = $transfer->getFileStatus($requestId,0,10);
      print Dumper($status);
      print Dumper($filestatus);
    }
    print Dumper($status);
    print Dumper($filestatus);
    print "Transfer done.\n";

  
  return 0;
}
return 1;


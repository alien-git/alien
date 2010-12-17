package AliEn::FTP::xrdcp;

use AliEn::SE::Methods::root;
use strict;
use vars qw(@ISA $DEBUG);

use AliEn::Logger::LogObject;
push @ISA, 'AliEn::Logger::LogObject';


use AliEn::FTP;
@ISA = ( "AliEn::FTP" );

sub initialize {
  my $self=shift;
  $self->info("This does the copy in two steps");

  $self->{MSS}=AliEn::SE::Methods->new({PFN=>'root://host/path',DEBUG=>$self->{DEBUG}}) 
    or $self->info("Error creating the interface to xrootd") 
      and return;

  return $self;
}

sub copy {
  my $self=shift;
  my $sEnvelope = shift;
  my $tEnvelope = shift;

  $self->info("Ready to copy $sEnvelope->{turl} into $tEnvelope->{turl} ");

  $self->{MSS}->{LOCALFILE}.=".$sEnvelope->{guid}";
  $ENV{ALIEN_XRDCP_URL}=$sEnvelope->{turl};
  $ENV{ALIEN_XRDCP_SIGNED_ENVELOPE}=$sEnvelope->{signedEnvelope};

  # if we have the old styled envelopes
  (defined($sEnvelope->{oldEnvelope})) and $ENV{ALIEN_XRDCP_ENVELOPE}=$sEnvelope->{oldEnvelope};

  $self->info("Issuing the get");
  my $file=$self->{MSS}->get();
  if (!$file) {
    $self->info("Error getting the file $sEnvelope->{turl}", 1);
    return ;
  }
  $self->info("Checking if it has the right size");

  my $size=-s $self->{MSS}->{LOCALFILE};
  if ($size ne $sEnvelope->{size}){
    $self->info("Error: the file was supposed to be $sEnvelope->{size}, but it is only $size",1);
    unlink $self->{MSS}->{LOCALFILE};    
    return;
  }
  
  $self->info("We got the file $file. Let's put it now in the destination");
  $ENV{ALIEN_XRDCP_URL}=$tEnvelope->{turl};
  $ENV{ALIEN_XRDCP_SIGNED_ENVELOPE}=$tEnvelope->{signedEnvelope};

  # if we have the old styled envelopes
  (defined($tEnvelope->{oldEnvelope})) and $ENV{ALIEN_XRDCP_ENVELOPE}=$tEnvelope->{oldEnvelope};

  if (!$self->{MSS}->put()){
    $self->info("Error putting the file $tEnvelope->{turl}");
    unlink $self->{MSS}->{LOCALFILE};
    return ;
  }
  $self->info("File copied!!");
  unlink $self->{MSS}->{LOCALFILE};
#  $self->info("Doing the command xrd3cp '$sEnvelope->{turl}?$sEnvelope->{envelope}' '$tEnvelope->{turl}?$tEnvelope->{envelope}'");
#  open (FILE, "xrd3cp '$sEnvelope->{turl}?$sEnvelope->{envelope}' '$tEnvelope->{turl}?$tEnvelope->{envelope}'|") or 
#    $self->info("Error doing the xrdc3p call!") and return;
#  my @info=<FILE>;
#  close FILE or 
#    $self->info("Error closing the xrd3cp call") and return;

#  print "Got @info\n";
  
  return 1;
}

1;

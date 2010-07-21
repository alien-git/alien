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
  my $source=shift;
  my $target=shift;
  $self->info("Ready to copy $source into $target ");

  $self->{MSS}->{LOCALFILE}.=".$source->{guid}";
  $ENV{ALIEN_XRDCP_ENVELOPE}=$source->{envelope};
  $ENV{ALIEN_XRDCP_URL}=$source->{url};

  $self->info("Issuing the get");
  my $file=$self->{MSS}->get();
  if (!$file) {
    $self->info("Error getting the file $source->{url}", 1);
    return ;
  }
  $self->info("Checking if it has the right size");

  my $size=-s $self->{MSS}->{LOCALFILE};
  if ($size ne $source->{size}){
    $self->info("Error: the file was supposed to be $source->{size}, but it is only $size",1);
    unlink $self->{MSS}->{LOCALFILE};    
    return;
  }
  
  $self->info("We got the file $file. Let's put it now in the destination");
  $ENV{ALIEN_XRDCP_ENVELOPE}=$target->{envelope};
  $ENV{ALIEN_XRDCP_URL}=$target->{url};
  if (!$self->{MSS}->put()){
    $self->info("Error putting the file $target->{url}");
    unlink $self->{MSS}->{LOCALFILE};
    return ;
  }
  $self->info("File copied!!");
  unlink $self->{MSS}->{LOCALFILE};
#  $self->info("Doing the command xrd3cp '$source->{url}?$source->{envelope}' '$target->{url}?$target->{envelope}'");
#  open (FILE, "xrd3cp '$source->{url}?$source->{envelope}' '$target->{url}?$target->{envelope}'|") or 
#    $self->info("Error doing the xrdc3p call!") and return;
#  my @info=<FILE>;
#  close FILE or 
#    $self->info("Error closing the xrd3cp call") and return;

#  print "Got @info\n";
  
  return 1;
}

1;

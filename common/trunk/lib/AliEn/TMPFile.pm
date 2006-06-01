package AliEn::TMPFile;
use strict;
use File::CacheDir;
use AliEn::Config;

my $global;
sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = ( shift or {} );
  bless( $self, $class );
  if (!$global){
    $global={CONFIG=>AliEn::Config->new()};
    $global->{CONFIG} or return;
    $global->{FILE}=File::CacheDir->new({ base_dir => $global->{CONFIG}->{TMP_DIR}, 
					  ttl => '2 hours', 
					  filename => 'alien_tmp.'.time.".$$",
					});
  }


  my $oldttl=$global->{FILE}->{ttl};
  $global->{CONFIG}->info("Creating a temporary file");

  my $old={};
  foreach my $key (keys %$self){
    $old->{$key}=$self->{$key};
    $global->{CONFIG}->info( "Setting $key of $self->{$key}");
    $global->{FILE}->{$key}=$self->{$key};
  }
  my $fileName;
  eval {
    $fileName=$global->{FILE}->cache_dir({ttl=>"2 seconds"});
  };

  foreach my $key (keys %$self){
    $global->{FILE}->{$key}=$old->{$key};
  }

  if ($@){
    $global->{CONFIG}->info("Error creating the temporary file $@");
    return;
  }
  return $fileName;
}


return 1;

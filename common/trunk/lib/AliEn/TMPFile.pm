package AliEn::TMPFile;
use strict;
use File::CacheDir;
use AliEn::Config;

my $global;
sub new {
  my $proto = shift;

  my $options=( shift or {});

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
  foreach my $key (keys %$options){
    $global->{FILE}->{$key} and $old->{$key}=$global->{FILE}->{$key};
    $global->{CONFIG}->info( "Setting $key of $options->{$key}");
    $global->{FILE}->{$key}=$options->{$key};
  }
  my $fileName;
  eval {
    $fileName=$global->{FILE}->cache_dir({ttl=>"2 seconds"});
  };
  my $error=$@;
  foreach my $key (keys %$options){
    $global->{FILE}->{$key}=$old->{$key};
  }

  if ($error){
    $global->{CONFIG}->info("Error creating the temporary file $@");
    return;
  }
  return $fileName;
}


return 1;

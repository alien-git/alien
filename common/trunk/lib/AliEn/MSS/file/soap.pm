package AliEn::MSS::file::soap;

@ISA= qw (AliEn::MSS::file);

use AliEn::MSS::file;
use AliEn::Config;
use strict;

sub url {
  my $config=new AliEn::Config;
  my $port=$config->{SE_PORT};
    my $self=shift;
    my $file=shift;
    return "soap://$self->{HOST}:$port$file?URI=SE";
}


return 1;




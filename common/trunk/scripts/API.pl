use strict;
use AliEn::API;
my $username = (shift or $ENV{'USER'});
my $api = new AliEn::API($username);
$api or print STDERR "Cannot start API for user <$username>!\n" and die;
$api->start();



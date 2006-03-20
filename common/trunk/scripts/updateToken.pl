use strict;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use strict;
use AliEn::Authen::IIIkey;
use IO::Socket;
use SOAP::Lite;
use AliEn::Config;

use Getopt::Long ();

my $ini = new AliEn::Config( { "NOPACKINSTALL", 1 } );
my $org = "\L$ini->{ORG_NAME}\E";

my $options = {'user' => $ENV{ALIEN_USER} };

( Getopt::Long::GetOptions( $options, "user=s", ) ) or exit;

my $user = $options->{user};

my $KEY;
my $tokenfile = "$ENV{HOME}/.alien/identities/token.$user";

( !-d "$ENV{HOME}/.alien" ) && mkdir( "$ENV{HOME}/.alien", 0755 );
( !-d "$ENV{HOME}/.alien/identities.$org" )
  && mkdir( "$ENV{HOME}/.alien/identities.$org", 0700 );
if ( open( TOKEN, "$tokenfile" ) ) {
    my @lines = <TOKEN>;
    close(TOKEN);
    $KEY = $lines[0];
}
else {
    $KEY = "AliGatorMasterKey";
}
my $passwd;
if ( !($passwd) ) {
    print STDERR"User: $user\n";
    print STDERR"Enter password:";
    system("stty -echo");
    chomp( $passwd = <STDIN> );
    system("stty echo");
    print("\n");
}

my $y               = new AliEn::Authen::IIIkey();
my $encryptedpasswd = $y->crypt( $passwd, $KEY );

my $host = $ini->{AUTH_HOST};
my $port = $ini->{AUTH_PORT};

my $done =
  SOAP::Lite->uri('AliEn/Service/Authen')->proxy("http://$host:$port")
  ->verify( $user, $encryptedpasswd );

if ( ( !$done ) or ( !$done->result ) ) {

    #Okay verification was not correct, so either the password really is wrong, or the secure serverkey has changed (ProxyServer restart).
    $KEY = "AliGatorMasterKey";
    my $encryptedpasswd = $y->crypt( $passwd, $KEY );
    $done =
      SOAP::Lite->uri('AliEn/Service/Authen')->proxy("http://$host:$port")
      ->verify( $user, $encryptedpasswd );
}
($done)
  or print STDERR "\nError connecting the the catalog in $host:$port\n"
  and return;

my $TOKEN = $done->result;
if ( !$TOKEN ) {
    print STDERR "Your password was wrong, or account non-existent.\n";
    exit;
}
else {
    open( TOKEN, ">$tokenfile" );
    print TOKEN $TOKEN;
    close(TOKEN);
    print STDERR "Your Alien token has been updated.\n";
}


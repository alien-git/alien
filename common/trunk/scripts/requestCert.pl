use strict;

use AliEn::X509;
use AliEn::Config;
use Getopt::Long;
use SOAP::Lite;
use AliEn::Authen::IIIkey;

my $conf = new AliEn::Config( { "NOPACKINSTALL", 1, "SILENT", 1 } );

$conf or exit;

my $org = "\L$conf->{ORG_NAME}\E";

my $options = {
    'user'  => $ENV{ALIEN_USER},
    'debug' => 0
};

my ($login,$pass,$uid,$gid,@p) = getpwnam($conf->{LOCAL_USER});

$ENV{ALIEN_USER} = @p[2] || "";

( Getopt::Long::GetOptions( $options, "help", "debug=n", "user=s" ) ) or exit;

my $username = $options->{'user'};

my $dir = "$ENV{ALIEN_HOME}/identities.$org";
(-d $dir) or (mkdir ($dir,0700) or print "Error doing the directory $dir\n" and exit );
print " Enter the password (AliEn Authentication server):\n";

my $passwd = $options->{password};
if ( !($passwd) ) {
    print STDERR" Username: $username\n";
    print STDERR" Enter password:";
    system("stty -echo");
    chomp( $passwd = <STDIN> );
    system("stty echo");
    print("\n");
}

my $y         = new AliEn::Authen::IIIkey;
my $encpasswd = $y->crypt( $passwd, "AliGatorMasterKey" );

print "Generating request....................";

my $KEY_FILE = "$dir/userkey.pem";
my $REQUEST_FILE = "$dir/userreq.pem";

my $config = "$ENV{ALIEN_ROOT}/ssl/alien-user-ssl.conf";

my $command="$ENV{ALIEN_ROOT}/bin/openssl req  -config $config -keyout $KEY_FILE -out $REQUEST_FILE";

system($command);

my $request = `cat $REQUEST_FILE`;

print "Requesting certificate....................";

my $done =
  SOAP::Lite->uri('AliEn/Service/Authen')
  ->proxy("http://$conf->{AUTH_HOST}:$conf->{AUTH_PORT}")
  ->requestCert( $org, $username, $encpasswd, $request );

if ( !($done) ) {
    print "FAIL\n";
    print "Could not contact $conf->{AUTH_HOST}:$conf->{AUTH_PORT}\n";
    exit;
}

my $res = $done->result;

if ( !($res) ) {
    print "FAIL\n";
    my @err = $done->paramsout;
    print "Server responded: $err[0]\n";
    exit;
}

if ( ($done) && ( $done->result ) ) {
    print "OK\n";
}

print "Writing the certificate...";
open (FILE ,">$dir/usercert.pem") or
  print "Error opening the file $dir/usercert.pem\n" and return;
print FILE $res;
close FILE;

print "ok\n";
chmod (400 , "$dir/usercert.pem");



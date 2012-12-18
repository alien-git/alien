use strict;

use AliEn::X509;
use AliEn::Config;
use Getopt::Long;

use AliEn::Authen::IIIkey;

my $l=AliEn::Logger->new();
use AliEn::RPC;

my $rpc=AliEn::RPC->new();


my $options = {
    'user'  => $ENV{ALIEN_USER},
    'debug' => 0,
    'organisation' =>  $ENV{ALIEN_ORGANISATION}
};

( Getopt::Long::GetOptions( $options, "help", "debug=s", "user=s", "organisation=s" ) ) or exit;

my $username = $options->{'user'};
my $org = lc($options->{organisation});

$options->{debug} and $l->setMinimum("debug", $options->{debug});

my $cert = new AliEn::X509;
my @dirs = ("$ENV{ALIEN_HOME}/identities.$org",  "$ENV{ALIEN_HOME}/globus");
my @certs;
my @keys;
my $dir;
foreach $dir (@dirs) {
    if ( ( -e "$dir/usercert.pem" ) && ( -e "$dir/userkey.pem" ) ) {
        push ( @certs, "$dir/usercert.pem" );
        push ( @keys,  "$dir/userkey.pem" );
    }
}

my $command = "openssl x509 -noout -subject -in";

my $num = 0;
print "*********************************************************\n";
print " Certificates:\n";
foreach $cert (@certs) {
    $num++;
    print "$num : ";
    system("$command $cert");
    print "\n";
}
if ( $num == 0 ) {
    print STDERR
"You do not have any certificates, or your ~/.alien/globus directory does not point to them\n";
    exit;
}
my $chosen = 1;

if ( $num > 1 ) {
    print "Please chose one:";
    chop( $chosen = <STDIN> );
    if ( $chosen > $num ) {
        print STDERR "Not a valid number\n";
        exit;
    }

}

$cert = new AliEn::X509;
$cert->load( $certs[ $chosen - 1 ] );

print "Subject: " . $cert->getSubject . "\n";

print "*********************************************************\n";
print " Enter the password to update your certificate in ";
print " AliEn Authentication server (CERN AFS pasword)\n";
my $passwd = $options->{password};
if ( !($passwd) ) {
    print STDERR" Username: $username\n";
    print STDERR" Enter password:";
    system("stty -echo");
    chomp( $passwd = <STDIN> );
    system("stty echo");
    print("\n");
}

my $KEY       = "AliGatorMasterKey";
my $y         = new AliEn::Authen::IIIkey;
my $encpasswd = $y->crypt( $passwd, $KEY );

print "Uploading subject....................\n";

my $done =$rpc->CallRPC("Authen", "insertCert", $org, $username, $encpasswd, $cert->getSubject );

$done or exit(-2);

print "OK Subject: "
      . $cert->getSubject
      . " succesfully uploaded to server\n";



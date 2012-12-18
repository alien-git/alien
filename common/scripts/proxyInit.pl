use strict;

use AliEn::Config;
use Getopt::Long;

my $conf = new AliEn::Config( { "NOPACKINSTALL", 1, "SILENT", 1 } );

my $org = "\L$conf->{ORG_NAME}\E";

my $options = {
    'user'  => $ENV{ALIEN_USER},
    'debug' => 0,
    'hours' => 25
};

(
    Getopt::Long::GetOptions(
        $options, "help", "debug=n", "user=s", "hours=n"
    )
  )
  or exit -2;

my $username = $options->{'user'};
my @dirs = ( "$ENV{HOME}/.alien/globus", "$ENV{HOME}/.alien/identities.$org" );
my @certs;
my @keys;
my $dir;
foreach $dir (@dirs) {
    if ( ( -e "$dir/usercert.pem" ) && ( -e "$dir/userkey.pem" ) ) {
        push ( @certs, "$dir/usercert.pem" );
        push ( @keys,  "$dir/userkey.pem" );
    }
}

my $command = "$ENV{ALIEN_ROOT}/bin/openssl x509 -noout -subject -in";

my $cert;
my $num = 0;
print "*********************************************************\n";
print " Certificates:\n";
foreach $cert (@certs) {
    $num++;
    print "$num : ";
    system("$command $cert");
    print "\n";
}
print "*********************************************************\n";

if ( $num == 0 ) {
    print STDERR
"You do not have any certificates, or your ~/.alien/globus directory does not point to them\n";
    exit -1;
}
my $chosen = 1;

if ( $num > 1 ) {
  print "Please chose one:";
  chop( $chosen = <STDIN> );
  if ( $chosen > $num ) {
    print STDERR "Not a valid number\n";
    exit -2;
  }

}

system(
"$ENV{ALIEN_ROOT}/bin/grid-proxy-init -hours $options->{'hours'} -cert $certs[$chosen-1] -key $keys[$chosen-1]"
);


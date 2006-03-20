
#!/usr/bin/perl

use strict;

use AliEn::Services::FTD;
use Getopt::Long;

my $options = { 'debug' => 0 };

( Getopt::Long::GetOptions( $options, "debug=n" ) ) or exit;

my $ftd = new AliEn::Services::FTD($options);

($ftd) or exit;

$ftd->startBBFTPD();
my $pid = fork();
( defined $pid ) or print STDERR "Error forking the proccess\n" and return;
if (!  $pid) {
    $ftd->startListening();

    # We should never come here
    print STDERR "We should never get here???\n";
}
$ftd->startChecking();


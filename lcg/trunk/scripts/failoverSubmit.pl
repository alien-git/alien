#!/usr/bin/perl -w
# failoverSubmit.pl -rb <rb1,rb2,...> [arguments]
# Does edg-job-submit to a failover list of RBs
# Patricia Mendez-Lorenzo, Stefano Bagnasco

use strict;
use Getopt::Long; 

my @rbs = ();
my $opt = new Getopt::Long::Parser;
$opt->configure("pass_through");
$opt->getoptions ( 'rb=s' => \@rbs) or exit 2;
@rbs = split(/,/, join(',', @rbs)); # Allow comma-separated list
@rbs or die "Error: no RB specified";
my @opts = @ARGV; # Passthrough
# Die if no RBs given?
my $defaults = "/opt/edg/etc/edg_wl_ui_cmd_var.conf";
if ( open DEFAULTS, "<$defaults" ) {
  while (<DEFAULTS>) {
    m/^\s*LoggingDestination/ and print "Warning: \'$_\' defined in $defaults\n";
  }
  close DEFAULTS;
}
my $rb_directory = "/home/alicesgm/.alien";
opendir SOMEDIR, $rb_directory or die "Cannot open directory $rb_directory\n";
while (my $name = readdir SOMEDIR){
  foreach my $thisRB ( @rbs ) {
    if( !-e "$rb_directory/$thisRB.vo.conf" ){
      open STVOCONF, ">$rb_directory/$thisRB.vo.conf" or die "Cannot open the RB file $rb_directory/$thisRB.vo.conf\n";
      print STVOCONF "[
        VirtualOrganisation = \"alice\";
        NSAddresses	    = \"$thisRB:7772\";
        LBAddresses	    = \"$thisRB:9000\";
        MyProxyServer	    = \"myproxy.cern.ch\"\n]\n";
      close STVOCONF;
    }
  }  
}
my $output = '';
my $error = 0;
redoit:foreach ( @rbs ) { 
  my $submission = "edg-job-submit  --config-vo $rb_directory/$_.vo.conf @opts";
  $output = `$submission`;
  $error = $?;
  if (!($output =~ /^https:/)){
    next redoit;  
  } else {
    last;
  }
}
print "$output";
exit $error;

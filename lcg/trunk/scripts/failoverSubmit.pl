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
    chomp;
    m/^\s*LoggingDestination/ and print STDERR "Warning: \'$_\' defined in $defaults\n";
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

my $lastGoodRB = $rbs[0];
if (open LASTGOOD, "<$rb_directory/.lastGoodRB") {
  my $last = <LASTGOOD>;
  chomp $last;
  foreach (@rbs) {
    if ($_ eq $last) {
      $lastGoodRB = $last;
      last;
    } #Don't use it if it's not in the current list
  }
  close LASTGOOD;
} 

my $error = submit("$rb_directory/$lastGoodRB.vo.conf", @opts);

if ( $error ) {
  redoit:foreach ( @rbs ) { 
    next redoit if ( $_ eq $lastGoodRB ); ##This one just failed
    $error = submit("$rb_directory/$_.vo.conf", @opts);
    next redoit if $error; 
    if ( open LASTGOOD, ">$rb_directory/.lastGoodRB" ) {
      print LASTGOOD "$_\n";
      close LASTGOOD;
    } else {
      print STDERR "Could not save $rb_directory/.lastGoodRB\n";
    }
    last;
  }
}
exit $error;

sub submit {
  my $file = shift;
  my $submission = "edg-job-submit --config-vo $file @_";
  my @output = `$submission`;
  $error = $?;
  (my $jobId) = grep { /https:/ } @output;
  $jobId =~ m/(https:\/\/[A-Za-z0-9.-]*:9000\/[A-Za-z0-9_-]{22})/;
  $jobId = $1;
  unless ( $error || !$jobId){
    chomp $jobId;
    print "$jobId";
  }
  return $error;
}

# failoverSubmit.pl -rb <rb1,rb2,...> [arguments]
# Does edg-job-submit to a failover list of RBs
# Patricia Mendez-Lorenzo, Stefano Bagnasco

use strict;
use Getopt::Long; 

my @rbs = ();
my $cfgdir = "$ENV{HOME}/.alien";
my $fallback = 120; #minutes
my $debug = 0;
my $opt = new Getopt::Long::Parser;
$opt->configure("pass_through");
$opt->getoptions ( 'rb=s'       => \@rbs,
                   'cfgdir=s'   => \$cfgdir, 
		   'fallback=i' => \$fallback,
		   'debugmode'  => \$debug ) or exit 2;
@rbs = split(/,/, join(',', @rbs)); # Allow comma-separated list
@rbs or die "Error: no RB specified";
my $rb_directory = glob($cfgdir);
my @opts = @ARGV; # Passthrough

my $defaults = "$ENV{EDG_LOCATION}/etc/edg_wl_ui_cmd_var.conf";
if ( open DEFAULTS, "<$defaults" ) {
  while (<DEFAULTS>) {
    chomp;
    m/^\s*LoggingDestination/ and print STDERR "FAILOVER: \'$_\' defined in $defaults\n";
  }
  close DEFAULTS;
}
opendir SOMEDIR, $rb_directory or die "FAILOVER: Cannot open directory $rb_directory\n";
while (my $name = readdir SOMEDIR){
  foreach my $thisRB ( @rbs ) {
    if( !-e "$rb_directory/$thisRB.vo.conf" ){
      print STDERR "FAILOVER: Config file for $thisRB does not exist, creating it...\n" if $debug;
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
print STDERR "FAILOVER: Default RB is $lastGoodRB\n" if $debug;
if ( -e "$rb_directory/lastGoodRB") {
  my $timestamp = (stat("$rb_directory/lastGoodRB"))[9];
  my $elapsed = (time-$timestamp)/60;
  print STDERR "FAILOVER: Last RB was first used $elapsed minutes ago.\n" if $debug;
  if ($elapsed > $fallback) {
    print STDERR "FAILOVER: This is more than $fallback\n" if $debug;    
  } else {
    if (open LASTGOOD, "<$rb_directory/lastGoodRB") {
      my $last = <LASTGOOD>;
      chomp $last;
      print STDERR "FAILOVER: Last RB was $last\n";
      foreach (@rbs) {
        if ($_ eq $last) {
          $lastGoodRB = $last;
          last;
        } #Don't use it if it's not in the current list
      }
      close LASTGOOD;
    } else {
      print STDERR "FAILOVER: Could not open $rb_directory/lastGoodRB\n";
    }
  } 
} else {
  if ( open LASTGOOD, ">$rb_directory/lastGoodRB" ) {
    print LASTGOOD "$lastGoodRB\n";
    close LASTGOOD;
  } else {
    print STDERR "FAILOVER: Could not save $rb_directory/lastGoodRB\n";
  }
}
print STDERR "FAILOVER: Will use $lastGoodRB\n" if $debug;

my $error = submit("$rb_directory/$lastGoodRB.vo.conf", @opts);

if ( $error ) {
  redoit:foreach ( @rbs ) { 
    next redoit if ( $_ eq $lastGoodRB ); ##This one just failed
    $error = submit("$rb_directory/$_.vo.conf", @opts);
    next redoit if $error; 
    if ( open LASTGOOD, ">$rb_directory/lastGoodRB" ) {
      print STDERR "FAILOVER: Found a good one, will use $_ from now on\n" if $debug;
      print LASTGOOD "$_\n";
      close LASTGOOD;
    } else {
      print STDERR "FAILOVER: Could not save $rb_directory/lastGoodRB\n";
    }
    last;
  }
}
exit $error;

sub submit {
  my $file = shift;
  my $submission = "edg-job-submit --config-vo $file @_";
  print STDERR "FAILOVER: Doing $submission\n" if $debug;
  my @output = `$submission`;
  $error = $?;
  (my $jobId) = grep { /https:/ } @output;
  unless ( $error || !$jobId){
    $jobId =~ m/(https:\/\/[A-Za-z0-9.-]*:9000\/[A-Za-z0-9_-]{22})/;
    $jobId = $1;
    chomp $jobId;
    print "$jobId";
  } else {
    print STDERR "FAILOVER: Error submitting to $file, trying next...\n";
  }
  return $error;
}

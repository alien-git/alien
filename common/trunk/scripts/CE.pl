use AliEn::CE;
use strict;
use AliEn::Config;

use strict;
use Getopt::Long ();

my $options = {
    'acronjob' => 0,
    'queue'    => "",
    'user'     => $ENV{ALIEN_USER},
    'role'     =>"",
    'debug'    => 0,
		'logfile'=>"",
};

Getopt::Long::Configure("pass_through");
#First, let's see if we have to redirect the output
Getopt::Long::GetOptions($options,"logfile=s"  )  or exit;

my $config = AliEn::Config->new({logfile=>$options->{logfile}});
$config or exit;
$options->{role}=$config->{CLUSTER_MONITOR_USER};


Getopt::Long::Configure("default");
Getopt::Long::GetOptions( $options,  "help", "acronjob", "user=s","password=s",
													"queue=s", "debug=n", "role=s","logfile=s",)
  or exit;

$config=$config->Reload({queue=>$options->{queue}, force=>1});


my $dir = $config->{LOG_DIR};

my $lock = "RemoteQueue." . $options->{queue} . ".lock";
my $log  = "RemoteQueue." . $options->{queue} . ".log";

if ( $options->{'acronjob'} ) {
    print "LOCKFILE $dir/$lock\n";
    ( -f "$dir/$lock" )
      and print STDERR "Error lock $dir/$lock exists\n"
      and exit;

    ( -d $dir ) or mkdir $dir, 07777;
    open FILE, ">$dir/$lock";
    close FILE;
}

my $cmhost = $config->{HOST};
my $cmport = $config->{CLUSTERMONITOR_PORT};

$options->{CM_HOST}=$cmhost;
$options->{CM_PORT}=$cmport;
$options->{MONITOR}=1;
#$options->{FORCED_AUTH_METHOD}="SSH";

my $base = AliEn::CE->new($options);
#    {
#        "user"     => $options->{user},
#        "debug"    => $options->{debug},
#        "password" => $options->{password},
#        "CM_HOST"  => $cmhost,
#        "CM_PORT"  => $cmport,
#        "queue"    => $options->{queue},
#     "FORCED_AUTH_METHOD" =>"SSH",
#    }
#);

($base) or exit;

if ( !$options->{'acronjob'} ) {
  my $count = 0;
  my $checkcount = 0;  
  my $file="$config->{LOG_DIR}/CE.env";
  $base->info("Putting the environment in $file");
  system("env >$file") and $base->info("Error opening the file $file");
  while (1) {
    ( $count == 120 ) and print "Asking for a job\n";

    if (! ($checkcount%30 ) ) {
      my $checkok = $base->checkQueueStatus();
      if (($checkok)) {
	print "Checked the Queue consistency!\n";
      } else {
	print "Checking of the Queue failed!\n";
      }
    }
    $checkcount++;
    my $ok = $base->offerAgent($count);

#    if ((!$ok) or ($ok and ($ok eq "-2"))) {
      $count++;
      ( $count == 120 ) and print STDERR "Sleeping ...\n" and $count=0;;
#    }
    sleep(60);

  }

}
else {

  print "Starting the RemoteQueue\n";
  my $ok = 1;
  my $checkcount = 0;
  while ($ok) {
    if (! ($checkcount%30 ) ) {
      my $checkok = $base->checkQueueStatus();
      $checkcount++;
      if (($checkok)) {
	print "Checked the Queue consistency!";
      } else {
	print "Checking of the Queue failed!";
      }
    }


    $ok = $base->offerAgent();
    sleep(60);
    $ok or $ok = "";
    print "RETURN CODE $ok\n";
  }

  print "Ending the RemoteQueue\n";
  unlink "$dir/$lock";
}


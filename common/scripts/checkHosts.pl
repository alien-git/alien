select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;
select(LOG);
$| = 1;

use strict;

use AliEn::Database;
use AliEn::UI::Catalogue;

#use AliEn::Services::ClusterMonitor;
#use AliEn::Broker;
use AliEn::Config;
use AliEn::Logger;
#use AliEn::Services::Sync;

my $config = new AliEn::Config();
$config or exit;
my $role = "admin";


my $dir  = "$config->{TMP_DIR}";
my $lock = "checkHosts.lock";
my $log  = "CheckHosts.log";

if ( -f "$dir/$lock" ) {
    print STDERR "Warning lock exists, killing the old process\n";
    open OLD, "<$dir/$lock";
    my $PID = <OLD>;
    close OLD;
    ($PID) and (`kill -9 $PID`);
}
open( LOG, ">$dir/$log" ) or print STDERR "Error creating the log $dir/$log\n";

my $Logger = new AliEn::Logger();

if ( !$Logger ) {
    print LOG "Error getting the Logger!!\n";
    close LOG;
    exit;
}

print LOG "Creating a lock\n";
( -d $dir ) or mkdir $dir, 07777;
open FILE, ">$dir/$lock";
print FILE "$$";
close FILE;

my $database = AliEn::Database->new(
    {
        "DB"     => $config->{"QUEUE_DATABASE"},
        "HOST"   => $config->{"QUEUE_DB_HOST"},
        "DRIVER" => $config->{"QUEUE_DRIVER"},
        "ROLE", $role,
    }
);

($database) or print LOG "Error getting the database!!\n";
( $database->validate() )
  or print LOG "Not validated!!\n"
  and close LOG
  and exit;


my $ISdatabase = AliEn::Database->new(
     {
	  "DB"     => $config->{"IS_DATABASE"},
	  "HOST"   => $config->{"IS_DB_HOST"},
	  "DRIVER" => $config->{"IS_DRIVER"}
      }
				      );
($ISdatabase) or print LOG "Error getting the database!!\n";
if ( !$ISdatabase->validateUser("admin") ) {
        print LOG "Not validated!!\n";
	$database->destroy();
        $ISdatabase->destroy();
        exit;
    }



#updateDatabases();


checkClusterMonitors($database);


#updateDatabases();

checkISElement("Services");
checkISElement("SE");
checkISElement("FTD");


$database->destroy();
print LOG "Deleting the lock\n";
unlink "$dir/$lock";
print LOG "DONE!!!!\n";
close LOG;

exit;

sub checkClusterMonitors {
    my $database = shift;
    my $date     = time;
    print LOG "\nStarting the update of the cluster status...";

    $database->query("UPDATE HOSTS set status='IDLE' where connected=0");
    $database->query("UPDATE HOSTS set connected=0");
    $database->query("UPDATE HOSTS set date='$date' where status='CONNECTED'");
    print LOG "done\n";
    return 1;
    my (@hosts) =
      $database->query(
        "SELECT hostName, hostPort from HOSTS where status<>'IDLE'");
    (@hosts) or print LOG "HOLA $DBI::errstr\n";
    my $host;

    foreach $host (@hosts) {
        my ( $name, $port ) = split "###", $host;

        print LOG "Contacting the local cluster monitor at $name:$port...";

        my $alive ="";
        my $date   = time;
        my $status = "status='IDLE'";
        my $where  = "hostName='$name'";

        if ($alive) {
            my $done = $alive->result or print LOG "Error getting result\n";
            my $version = $done->{VERSION};
            print LOG "alive with version $version!!\n";
            $status = "status='CONNECTED', date='$date', Version='$version'";
            my $newdate = $date - 400;
            $where = "$where and (status='CONNECTED' or date<$newdate)";
        }
        else {
            print LOG "dead!!\n";
        }

        $database->insert("UPDATE HOSTS set $status where $where");

    }
}

sub checkISElement {
    my $table = shift;

    my $query = "SELECT host, port, name, URI from $table where status='ACTIVE'";

    print LOG "\nStarting the update of the $table...\n";
    my (@hosts) = $ISdatabase->query($query);

    (@hosts) or print LOG "Problems with $query\n$DBI::errstr\n";
    my $host;
    foreach $host (@hosts) {
        my ( $name, $port, $serviceName, $uri ) = split "###", $host;

	$uri =~ s/::/\//g;
        print LOG "Checking $uri $name:$port ....";
	
        my $alive ="";

        if ($alive) {

	    if (($serviceName eq "IS") or ($serviceName eq "CPUServer")){
		$alive = $alive->ping();
	    } else {
		$alive =$alive->alive();
	    }

	 }
        my $date    = time;
        my $status  = "status='IDLE'";
        my $message = "dead!!\n";
        if ($alive) {
            my $done = $alive->result or print LOG "Error getting result\n";
            if ( $done->{VERSION} ) {
                my $version = $done->{VERSION};
                $message = "alive with version $version!!\n";
                $status  = "lastchecked='$date', Version='$version'";
            }
        }
        print LOG "$message";
        my $query = "UPDATE $table set $status where name='$serviceName' and host='$name'";
	print "DOING $query\n";
        $ISdatabase->insert($query)
          or print STDERR "ERROR DOING $query\n $DBI::errstr\n";

    }

    return 1;
}

sub updateDatabases {

    print LOG "\n\nUpdating the databases...\n";
    my $db = AliEn::Database->new(
        {
            "DB"     => $config->{CATALOG_DATABASE},
            "HOST"   => $config->{CATALOG_HOST},
            "DRIVER" => $config->{CATALOG_DRIVER},

            "SILENT" => $config->{SILENT},
            "ROLE", "admin"
        }
    );

    ($db) or exit;
    $db->validate() or exit;
    $db->{noproxy} = 1;
    print STDERR "Update of whole database\n";
    AliEn::Services::Sync->updateDatabase($db);
    print STDERR "Update done!!\n";
    $db->destroy;

    return 1;
}


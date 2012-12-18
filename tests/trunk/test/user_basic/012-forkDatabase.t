use strict;

use AliEn::Database;
use Net::Domain qw(hostname hostfqdn hostdomain);
push @INC, $ENV{ALIEN_TESTDIR};
require functions;

my $org=Net::Domain::hostname();
setDirectDatabaseConnection();

my $d=AliEn::Database->new({"HOST", "$org:3307", "DB", "alien_system", "DRIVER", "mysql", });

$d or exit(-2);

print "Forking...\n";
$d->{LOGGER}->debugOn("Database");
my $pid=fork;
defined $pid or exit(-2);
if($pid) {
  #the father
  print "father queries\n";
  my $l=$d->query("SELECT * from L0L, L0L as D1, L0L as D2 limit 100000") or exit(-2);
  print "The father got\n";
  print "father got $#{$l}\n";
  print "YUHUU\n";
}else {
  sleep(1);
  my $l=$d->queryValue("SELECT count(*) from L0L");
  print "got $l\n";
  $l=$d->queryValue("SELECT count(*) from L0L");
  print "got $l\n";
  $l=$d->queryValue("SELECT count(*) from L0L");
  print "got $l\n";
  $d->close();
  exit;
}
local $SIG{ALRM} =sub {
  print "The process $pid did not finish (let's kill it):(\n";
  kill 9, $pid;
  die("nope  ");
};
alarm(30);
waitpid($pid,0);
alarm(0);
print "The father does the query again\n";
my $l=$d->query("SELECT * from L0L, L0L as D1, L0L as D2 limit 10000") or exit(-2);

alarm(30);
print "father got $#{$l}\n";
print "Father quits\n";

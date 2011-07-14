use strict;
use Test;
BEGIN { plan tests => 1 }

push @INC, $ENV{ALIEN_TESTDIR};
require functions;
setDirectDatabaseConnection();
use AliEn::Database;

my $c = new AliEn::Config;
my $d = AliEn::Database->new(
  { DB     => $c->{CATALOG_DATABASE},
	HOST   => $c->{CATALOG_HOST},
	DRIVER => $c->{CATALOG_DRIVER},
	DEBUG  => "Database"
  }
);

$d or exit(-1);

my $id = fork();
defined $id or print "ERROR doing the fork\n" and exit(-2);

if (!$id) {
  print "The child does a wrong query\n";
  my $result = $d->query("bla bla bla");
  print "The query returned " . ($result || "undef") . "\n";

  exit(0);
}

sleep(5);
print "The father ($$) checks if the children ($id) exists\n";
system("ps -o \"pid ppid command\"");
my $exists = kill 0, $id;

if ($exists) {
  print "THE CHILDREN IS STILL THERE!!!!\n";
  system("ps -p $id");
  my @defunct = `ps -o "state" -p $id`;

  kill 9, $id;
  $defunct[1] =~ /Z/ or print "It isn't defunct\n" and exit(-2);
}
print "The children is not there any more\n";
ok(1);


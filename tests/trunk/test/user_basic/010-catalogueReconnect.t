use strict;
use warnings;

use AliEn::Database::Catalogue;

my $l=AliEn::Logger->new({DEBUG=>5});
my $db=AliEn::Database::Catalogue->new({DEBUG => 5,debug =>5}) or exit(-2);

print "Got the database\n";

my @list=$db->query("select count(*) from L0L");
print @list;

system("/opt/alien/etc/rc.d/init.d/alien-mysqld stop");
sleep(5);
system("/opt/alien/etc/rc.d/init.d/alien-mysqld start");

sleep(5);

print "AFTER THE RESTART\n";

my @list2=$db->query("select count(*) from L0L");
print @list2;

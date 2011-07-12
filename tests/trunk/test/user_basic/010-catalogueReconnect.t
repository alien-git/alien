use strict;
use warnings;

use AliEn::Database::Catalogue;


my $db=AliEn::Database::Catalogue->new({ROLE=>'admin', PASSWD=>'pass', 'user'=>'admin'}) or exit(-2);

print "Got the database\n";

my @list=$db->queryValue("select count(*) from INDEXTABLE");
print @list;

system("/opt/alien/etc/rc.d/init.d/alien-mysqld stop");
sleep(5);
system("/opt/alien/etc/rc.d/init.d/alien-mysqld start");

sleep(5);

print "AFTER THE RESTART\n";

my @list2=$db->queryValue("select count(*) from INDEXTABLE");
print @list2;

use strict;

use AliEn::Database::Catalogue;

use AliEn::UI::Catalogue::LCM;
system("ps -eo command |grep -i Proxy| grep -v grep |wc -l");


my $d=AliEn::Database::Catalogue->new() or exit(-2);

system("ps -eo command |grep -i Proxy|grep -v grep ");

sleep (5);
$d->disconnect();
print "closed!!\n";

my $i=5;
while ($i) {
#sleep (5);
 print "I'm $$ and the children is \n";
system("ps -eo command |grep -i Proxy|grep -v grep |wc -l");
system("netstat -p 2> /dev/null |grep $$");
sleep (5);
  $i--;
}


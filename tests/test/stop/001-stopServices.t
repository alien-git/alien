use strict;


use Test;


BEGIN { plan tests => 1 }


print "Stopping everything\n";

system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/aliend stop");
system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-mysqld  stop");
system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-ldap stop");

print "Everything is supposed to be dead now...\n";

system("ps -ef | grep $ENV{ALIEN_ROOT} | grep -v grep");


print "Are we happy? For the time being, we return error to keep the output\n";
exit(-2);
ok(1);

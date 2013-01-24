use strict;


use Test;



my $file="$ENV{ALIEN_ROOT}/bin/monit";
print "Checking if the file '$file' is there...";

(-f $file) or print "Error $file does not exist\n" and exit(-2);

print "ok\nTrying to start the monitor service...";

system("$ENV{ALIEN_ROOT}/bin/alien", "-x", "$ENV{ALIEN_ROOT}/scripts/Monitor/Install.pl") and print "ERROR doing '$ENV{ALIEN_ROOT}/bin/alien -x $ENV{ALIEN_ROOT}/scripts/Monitor/Install.pl' $! $? \n" and exit(-2);
print "ok\n";

my $file2="/tmp/crontab.new.$$";
system("crontab -l | grep -v Monitor |grep -v '#'>> $file2");
system("crontab $file2");

print "ok!\n";

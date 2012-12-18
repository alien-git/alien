use strict;
my $date = time;

print "Time " . localtime($date) . "\n  ";
print "Sleeping for 60 seconds...\n";
sleep(60);
print "Time " . localtime($date) . "\n  ";
print "Done!!!\n";


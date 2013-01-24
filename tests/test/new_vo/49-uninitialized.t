use strict;
use AliEn::Config;

my $c=AliEn::Config->new();

open (FILE, "grep 'Use of uninitialized' /var/log/AliEn/$c->{ORG_NAME}/*.log|") 
	or print "Error getting the lines \n$! $?" and exit(-2);

my @lines=<FILE>;
close FILE;# and print "Error doing the grep\n$! $?\n" and exit(-2);

$#lines>-1 and print "Error: there are some uninitialized variables!!\n@lines\n" and exit(-2);

print "Everything is fine :D\n";

exit(0);
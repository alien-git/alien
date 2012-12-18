#!/bin/env alien-perl

use strict;
use Test;
use AliEn::Config;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }

use AliEn::UI::Catalogue::LCM::Computer;
{
	my $cat=AliEn::UI::Catalogue::LCM::Computer->new();
	$cat or print "Error getting the catalogue\n" and exit (-1);

	my @jobs=$cat->execute("top", "-status", "DONE");
	@jobs or print "There are no finished jobs!!\n";

	my $id=$jobs[$#jobs];
	$id =~ s/\#.*$//;
#	my $id=27;
	print "Getting the output of job $id\n";

  my $config=new AliEn::Config;

	print "Checking if lynx exists...";
	system(" lynx -version > /dev/null") and print "Error! lynx is not in the path\n$! $?\n" and exit(-2);

	my $host=Net::Domain::hostfqdn();
	
	my $page="http://$host/$config->{ORG_NAME}/main?task=job&jobID=$id";
	print "ok\nGetting the default page $page...";
	open (FILE, "lynx -dump '$page' |") 
		or print "Error doing lynx!!\n$! $?\n" and exit(-2);

my @output=<FILE>;

close (FILE)  or print "Error closing lynx!!\n$! $?\n" and exit(-2);

print "GOT @output\n";
@output or print "Error: we didn't get any output" and exit(-2);

grep ( /No output to stdout so far/, @output) and print "Error: there is no output for the job $id\n" and exit(-2);
ok(1);
}

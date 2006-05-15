use strict;
use Test;

BEGIN { plan tests => 1 }


{
	print "Testing the API\n";
	
	my $home="/home/alienmaster/AliEn";

	$ENV{PREFIX}=$ENV{ALIEN_ROOT};
	chdir ("$home/API/C") or print "Error going to $home/API/C\n" and exit(-1);
	system("make") and print "Error doing make \n" and exit(-1);
	
	chdir("test") or print "Error going to $home/API/C/test\n" and exit(-1);
	system("make") and print "Error doing make \n" and exit(-1);
	my $user="alienmaster";
	$ENV{LD_LIBRARY_PATH}="$ENV{ALIEN_ROOT}/lib/api:$ENV{LD_LIBRARY_PATH}";
	system("./Test", $user) and print "Error connecting\n" and exit(-1);
	ok(1);

}

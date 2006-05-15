use strict;

use Test;
BEGIN { plan tests => 1 }

{
	my $file="$ENV{ALIEN_ROOT}/bin/imlib2-config";
	-f $file or print "Error file $file does not exist!!\n" and exit(-2);

	my $d=`$file --cflags`;
	print "Got $d\n";
	$d =~ /\/opt\/alien\/include/ and print "Error: $file is requiring things from /opt/alien/, although the installation is in $ENV{ALIEN_ROOT}\n" and exit (-3);

ok (1);

}

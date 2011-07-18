use strict;

use Test;

use AliEn::UI::Catalogue::LCM::Computer;

my $id1=shift or print "Error getting the id\n" and exit(-2);

BEGIN { plan tests => 1 }

my $user="FQUser";
my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
  $cat or exit(-1);
my ($log)=$cat->execute("ps", "trace", $id1);
my $found=0;
foreach my $entry (@$log){
	if ($entry->{trace}=~ /THERE WAS AT LEAST ONE FILE, THAT WE COULDN'T STORE ON ANY SE/) {
		$found=1;
		last;
	}
};
$found or print "There is nothing in the log about quota overflow\n" and exit(-2);
print "4. PASSED\n\n";


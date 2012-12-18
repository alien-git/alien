use strict;

use AliEn::X509;

use AliEn::UI::Catalogue::LCM;

system ("alien proxy-init") and exit(-2);

system("alien proxy-info") and exit(-2);
#system("alien proxy-destroy");
my $cat=AliEn::UI::Catalogue::LCM->new({user=>"newuser", 
	"FORCED_AUTH_METHOD"=>"GSSAPI", 
	debug=>5}) or exit(-2);

print "Connected with certificate\n";

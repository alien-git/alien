use strict;

use AliEn::UI::Catalogue;

my $c=AliEn::UI::Catalogue->new({USER=>"newuser"
                                }) or exit (-2);

print "Deleting a file that does not exist...\n";
$c->execute("rm", "blablabla_bbadas") and print "it was not supposed to work :( \n" and exit(-2);
$c->close();

print "ok\n"

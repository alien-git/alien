use AliEn::Classad::Host;
use strict;
use Test;
BEGIN { plan tests => 1 }



{
  my $ca=AliEn::Classad::Host->new() or exit(-2);

  my $text=$ca->asJDL();



  print "TENGO $text\n";

  $ca->asJDL() =~ /LocalDiskSpace/s 
    or print "Error the diskspace is not there\n" and exit(-2);


};
  

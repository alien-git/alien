#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("catalogue/003-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM->new({"role", "admin"});
  $cat or exit (-1);
  
  my $c=new AliEn::Config;
  my $org="\L$c->{ORG_NAME}\E";
  my $dir="/$org/tags";
  
  $cat->execute("mkdir", "-p", "$dir") or exit(-2);
  
   addFile($cat, "$dir/person","firstname char(40), familyName char(40), year int\n") or exit(-2);

  my  $targetDir="/$org/user/n";
  my @tags=$cat->execute("showTags", $targetDir);
  
  if (grep (/^person$/, @tags)) {
    print "Got @tags";
    $cat->execute("removeTag", $targetDir, "person") or exit(-2);
  }
  $cat->execute("debug","5");
  $cat->execute("addTag", $targetDir, "person") or exit(-2);
  $cat->execute("debug","0");
  
  $cat->execute("addTagValue", "$targetDir/newuser", "person", "firstname=James",
		"familyName=Smith", "year=1950") or exit(-2);
  
  
  $cat->close();
  
  ok(1);
}

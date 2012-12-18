#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"role", "admin",});
  $cat or exit (-1);
  $cat->execute("queue", "priority", "add", "newuser"); 
  $cat->close();
  $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);

  $cat->execute("cd") or exit (-2);
  $cat->execute("pwd") or exit (-2);
  $cat->execute("mkdir", "-p", "jdl") or exit(-2);
  addFile($cat, "jdl/date.jdl","Executable=\"date\";\n") or exit(-2);
  my ($id)=$cat->execute("submit", "jdl/date.jdl");
  $id or exit(-2); ;

  $cat->execute("top") or exit(-2);

  $cat->execute("ps") or exit(-2);

  print "Checking if there are any warnings during the top...";
	

  ok(1);
}

#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);
  my $id=shift;
  my $id2=shift;

  ($id and $id2) or print "Error getting the ids\n" and exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);

  checkEmail($cat, $id, "DONE") or exit(-2);
  checkEmail($cat, $id, "ERROR_IB") or exit(-2);
  $cat->close();
  ok(1);
}


sub jobWithEmail {
  my $cat=shift;
  my $id=shift;
  my $status=shift;

  my ($trace)=$cat->execute("ps", "trace", $id, "all") or return;
  use Data::Dumper;
  foreach my $entry (@$trace){
    $entry->{trace}=~ /Sending an email to (\S+) \(job (\S+)\)/ or next;
    $status =~ /$1/ and print "The job sent the right email\n" and return 1;
    print "The job sent an email to $1, with status $2\n";
    return;
  }
  print "The job didn't send any emails\n";
  print Dumper($trace);
  return;
}

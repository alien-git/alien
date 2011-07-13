#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  
  my $cat=AliEn::UI::Catalogue::LCM->new({"user"=>"newuser"});
  $cat or exit (-1);

  my $targetDir="tagInfo";
  my $entries=100;
  my $add=1;
  if ($add) {
    $cat->execute("rmdir","-rf", $targetDir);

    $cat->execute("mkdir", $targetDir) or exit(-2);
    $cat->execute("addTag", $targetDir, "person") or exit(-2);
    my $before=time;
    $cat->execute("silent");
    #Populate the catalogue with 100 entries;
    for (my $i=0; $i<$entries; $i++) {
      my $file=sprintf("$targetDir/f$i");
      $cat->execute("touch", $file) or exit(-2);
      $cat->execute("addTagValue", $file, "person", "familyName='saiz'", "year=".($i%2)) or exit(-2);
      print ".";
      $i %50 or print "\n";
    }
    $cat->execute("silent");

    my $after=time;
    print "\nIt took ". ($after -$before). " seconds to add $entries entries\n";
  }
  my $total=0;
  my $start=$entries;
  while ( $start>0.5){
    print "DOING $start and $total";
    $total+=$start/20;
    $start/=10;
  }
  $total=POSIX::ceil($total);

  my @queries=({query=> ["f", ], result=>$entries},
	       {query=> ["f", "person:year=1"], result=>$entries/2},
	       {query=> ["f9", "person:year=1"], result=>$total});
  foreach (@queries) {
#    my @list=
    print "Searching for ".join(" ", @{$_->{query}}) ."\n";
    #$cat->execute("debug", 5);
    my $match=$cat->execute("find", $targetDir, @{$_->{query}});

    print "Got $match, and I should have got $_->{result}\n";
    ($match eq $_->{result}) or exit(-2);
  }

# Finally, let's retrieve some metadata
#
#
  checkMetadata($cat, "$targetDir/f21", {familyName=>"saiz", year=>1}) 
    or exit(-2);
  $cat->execute("mkdir", "-p", "$targetDir/d1") or exit(-2);
  $cat->execute("touch", "$targetDir/d1/otherFile") or exit(-2);
  $cat->execute("addTagValue", "$targetDir/d1", "person", "familyName='saiz'", "year=3") or exit(-2);

  checkMetadata($cat, "$targetDir/d1", {familyName=>"saiz", year=>3}) 
    or exit(-2);
  checkMetadata($cat, "$targetDir/d1/", {familyName=>"saiz", year=>3}) 
    or exit(-2);

  checkMetadata($cat, "$targetDir/d1/otherFile", 
		{familyName=>"saiz", year=>3}) 
    or exit(-2);

  $cat->close();

  ok(1);
}

sub checkMetadata{
  my $cat=shift;
  my $lfn=shift;
  my $metadata=shift;
  print "\n\n\n\nIn checkMetadata of $lfn\n";
  my ($def, $data)=$cat->execute("showTagValue", $lfn, "person") or return;
  print "AFTER showTagValue\n";
  my $entry=shift @$data;
  foreach (keys %$metadata){
    print "Checking $_ -> $metadata->{$_}\n";
    $entry->{$_} eq $metadata->{$_} or 
      print "The metadata is not what we expected!! $entry->{$_} and $metadata->{$_}\n" and return;
  }
  return 1;
}

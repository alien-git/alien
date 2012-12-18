use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);
  $cat->execute("rmdir", "-rf", "copyMetadata", "-silent");
  
  my $source="copyMetadata/source/file1";
  $cat->execute("mkdir", "-p", "copyMetadata/source", "copyMetadata/target", "tags") or exit(-2);
  print"Directory created\n";
  addFile($cat, $source, "test for copying metadata") or exit(-2);
  print "File touched";
  addFile($cat, "tags/AnUserTag","my_value varchar(200)") or exit(-2);
  print "Adding the tag\n";
  $cat->execute("addTag","copyMetadata/source", "AnUserTag") or exit(-2);
  print "Assing the tagValue\n";
  $cat->execute("addTagValue", $source, "AnUserTag", "my_value='sipe'") or exit(-2);
  print "So far so good. Let's try the real copy\n";

  $cat->execute("cp", "-m", $source, "copyMetadata/target") or exit(-2);

  print "The copy worked!! Let's see if we have the metadata\n";

  my ($columns, $info)=$cat->execute("showTagValue",  "copyMetadata/target/file1", "AnUserTag") or exit(-2);
  print "Got the metadata\n";
  use Data::Dumper;
  print Dumper ($info);

  $$info[0]->{my_value} eq 'sipe' or print "The metadata is not there!!\n" and exit(-2);

  print "Let's try now a copy of the directory...\n";
  
  $cat->execute("cp", "-m", "copyMetadata/source", "copyMetadata/target2") or 
    exit(-2);
  print "Does it have the metadata?\n";

  ($columns, $info)=$cat->execute("showTagValue",  "copyMetadata/target2/file1", "AnUserTag") or exit(-2);
  print "Got the metadata\n";
  use Data::Dumper;
  print Dumper ($info);
  $$info[0]->{my_value} eq 'sipe' or print "The metadata is not there!!\n" and exit(-2);

  print "ok\n";
  
}

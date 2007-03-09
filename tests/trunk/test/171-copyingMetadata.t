use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);
  $cat->execute("rmdir", "-rf", "copyMetadata", "-silent");
  
  my $source="copyMetadata/source/file1";
  $cat->execute("mkdir", "-p", "copyMetadata/source", "copyMetadata/target", "tags") or exit(-2);
  print"Directory created\n";
  $cat->execute("touch", $source) or exit(-2);
  print "File touched";
  addFile($cat, "tags/AnUserTag","my_value varchar(200)") or exit(-2);
  $cat->execute("addTag","copyMetadata/source", "AnUserTag") or exit(-2);
  $cat->execute("addTagValue", $source, "AnUserTag", "my_value='sipe'") or exit(-2);
  print "So far so good. Let's try the real copy\n";

  $cat->execute("cp", "-m", $source, "copyMetadata/target") or exit(-2);

  print "The copy worked!! Let's see if we have the metadata\n";

  my ($columns, $info)=$cat->execute("showTagValue",  "copyMetadata/target/file1", "AnUserTag") or exit(-2);
  print "Got the metadata\n";
  use Data::Dumper;
  print Dumper ($info);


  
}

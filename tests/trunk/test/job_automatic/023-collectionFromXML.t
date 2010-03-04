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
  $cat->execute("rm", 'collections/collection_from_xml', '-silent');

  print "First, let's see if we can create a collection from an xml file...\n";
  $cat->execute("createCollection", 'collections/collection_from_xml', '-xml', 'splitDataset/list.xml') or exit(-2);
  print "The collection is there\n";
  my ($files)=$cat->execute("listFilesFromCollection", "collections/collection_from_xml") or exit(-2);
  $#$files eq "1" or print "There are $#$files files, and there were supposed to be 2!!\n"  and exit(-2);

  addFile($cat, "jdl/collectionFormat.jdl","Executable=\"SplitDataset.sh\";
Split=\"file\";
InputDataCollection=\"LF:${dir}/collections/collection_from_xml\";
InputDataList=\"mylocallist.xml\";
InputDataListFormat=\"merge:${dir}/splitDataset/list.xml\"
",'r') or exit(-2);

  print "And now, let's try submitting a job with splitdatalistformat\n";
  my ($id)=$cat->execute("submit", "jdl/collectionFormat.jdl") or exit(-2);

  print "DONE!!
\#ALIEN_OUTPUT $id\n";
  
}

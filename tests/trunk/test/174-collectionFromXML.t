use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);
  includeTest("86-split") or exit(-2);

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
  my ($ok, $procDir, $subjobs)=executeSplitJob($cat, "jdl/collectionFormat.jdl") or exit(-2);

  $subjobs eq "2" or print "The job is not split in 2 subjobs\n" and exit(-2);

  my ($user)=$cat->execute("whoami") or exit(-2);

  print "\n\nlet's check the output\nWe got back $ok, $procDir and $subjobs\n";
  my $subJobDir="$procDir/subjobs";
  my @dirs=$cat->execute("ls", $subJobDir) or exit(-2);
  my $second=0;
  foreach my $entry (@dirs) {
    $entry =~ /job-log/ and next;
    print "Checking the output of $entry\n";
    my ($file)=$cat->execute("get", "$subJobDir/$entry/job-output/stdout") or exit(-2);
    open (FILE, "<$file") or print "Error opening $file\n" and exit(-2);
    my @content=<FILE>;
    close FILE;
    grep (/evlist/, @content) or print "There are no evlist in the file!!\n" and exit(-2)
  }

  print "ok\n";

}

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("86-split") or exit(-2);

  my $id=shift;
  $id or print "No job to analyze!!\n" and exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);

  my ($procDir)=checkSubJobs($cat, $id, 2) or exit(-2);

  print "\n\nlet's check the output\nWe got back  $procDir\n";
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

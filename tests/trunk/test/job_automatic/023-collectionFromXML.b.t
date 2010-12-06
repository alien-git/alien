use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  my $id=shift;
  
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  
  includeTest("job_automatic/008-split") or exit(-2);
   
   
  $id or print "Error: there is no job id\n" and exit(-2);
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($user)=$cat->execute("whoami") or exit(-2);
  my ($info)=$cat->execute("masterJob", $id) or exit(-2);
  my $subjobs=0;
  foreach my $s (@$info){
    $subjobs+=$s->{count};
  }
  $subjobs eq "2" or print "The job is not split in 2 subjobs\n" and exit(-2);

  my ($procDir, $ids)=checkSubJobs($cat, $id, 2) or exit(-2);
  print "\n\nlet's check the output\nWe got back $procDir and $subjobs\n";
  
  foreach my $entry (@{$ids->{DONE}}) {

    print "Checking the output of $entry\n";
    my ($file)=$cat->execute("get", "~/alien-job-$entry/job-output/stdout") or exit(-2);
    open (FILE, "<$file") or print "Error opening $file\n" and exit(-2);
    my @content=<FILE>;
    close FILE;
    grep (/evlist/, @content) or print "There are no evlist in the file!!\n" and exit(-2)
  }

  print "ok\n";

}

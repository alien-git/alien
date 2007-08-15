use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{

  my $id=shift or print "Error getting the job id\n" and exit(-2);
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("26-ProcessMonitorOutput") or exit(-2);
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);


  my $procDir=checkOutput($cat, $id) or exit(-2);
  print "The job executed properly!!\n";
  my ($out)=$cat->execute("get","$procDir/job-output/stdout") or exit(-2);
  system("cat", "$out");
  system("grep 'YUHUUUUU' $out") and
  print "The line is not there!!!" and exit(-2);

  print "Let's check the log file to see if the file was staged\n";
  my ($log)=$cat->execute("get", "$procDir/job-log/execution.out") or exit(-2);
  open (FILE, "<$log") or print "Error opening the file $log\n" and exit(-2);
  my @content=<FILE>;
  close FILE;
  print "Got @content\n";
#  grep (/staging the file/, @content) or print "The file was not staged!!\n" and exit(-2);
  print "ok\n";
  exit;
}

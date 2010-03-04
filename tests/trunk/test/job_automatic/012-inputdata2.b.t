use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{

  my $id=shift or print "Error getting the job id\n" and exit(-2);
  my $id2=shift or print "Error getting the job id\n" and exit(-2);

  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);
  my ($se)=$cat->execute("echo", "SE_FULLNAME") or exit(-2);

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
  getRequirements($cat, $id, $se) or exit(-2);
  print "ok\nLet's check the second job\n";
  my $procDir2=checkOutput($cat, $id2) or exit(-2);
  print "The job executed properly!!\nWas there a requirement about the se??\n"
;
  getRequirements($cat, $id2, $se) or exit(-2);
  print "ok!\n";
  exit;
}

sub getRequirements{
  my $cat=shift;
  my $id=shift;
  my $se=shift;
  my ($info)=$cat->execute("ps", "jdl", $id) or return;
  
  print "The jdl is '$info'\n";

  $info=~ /Requirements\s*=([^;]*);/i or return;
  print "The requirements are $1\n";
  $1 =~ /member\(other.CloseSE,"$se"\)/i or print "The requirements don't include restriction on '$se'\n" and return;;
  return 1;
  

}

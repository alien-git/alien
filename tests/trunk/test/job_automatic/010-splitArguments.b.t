use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("job_automatic/008-split") or exit(-2);

  my $id=shift or print "No job to analyze!!\n" and exit(-2);
  my @files=@ARGV;
  print "And the files are @files\n";

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);

  my ($procDir, $ids)=checkSubJobs($cat, $id,4) or exit(-2); 

  my ($user)=$cat->execute("whoami") or exit(-2);

  print "\n\nlet's check the output\nWe got back $procDir\n";
  my $subJobDir="$procDir/subjobs";
  
  my $second=0;
  foreach my $id (@{$ids->{DONE}}) {
    my $entry="~/alien-job-$id";
    print "Checking the output of $entry\n";
    my ($file)=$cat->execute("get", "$entry/job-output/stdout") or exit(-2);
    open (FILE, "<$file") or print "Error opening $file\n" and exit(-2);
    my @content=<FILE>;
    close FILE;
    my ($line)=grep (s/^.*ve been called with \'//, @content);
    $line or print "There is no output in job $entry\n" and exit(-2);
    chomp $line;
    print "GOT $line\n";
    $line=~ /second round/  and ++$second and next;
    $line=~ /allfiles: (\S+) /
      or print "Error there are no files\n" and exit(-2);
    foreach my $entry (split (",", $1)){
      grep (/^$entry$/, @files) 
	or print "The file $entry was not there originally (@files)\n" and exit(-2);
      @files=grep ( ! /^$entry$/, @files);
    }
  }

  $second == 2 or print "There are  $second entries with second round, and there should 2\n" and exit(-2);
  print "The files @files were in the input but were not processed\n";
  (@files) and exit(-2);
  print "ok\n";
}

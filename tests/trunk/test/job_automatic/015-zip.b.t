use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

my $id=shift or print "Error getting the id of the job\n" and exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user"=>"newuser"}) or exit(-2);



my $procDir=checkOutput($cat, $id)  or exit(-2);


print "JOB DONE ($procDir)!!\n\n";
my ($output)=$cat->execute("get", "$procDir/job-output/stdout", "-silent") or print "Error getting the output\n" and exit(-2);

$cat->close();

open (FILE, "<$output") or print "Error opening the file $output\n";
my @content=<FILE>;
close FILE;

print @content;

grep (/ file1$/, @content) or print "Error the file file1 is not there!!!!\n" and exit(-2);
grep (/ file2$/, @content) or print "Error the file file1 is not there!!!!\n" and exit(-2);

print "ok\n";

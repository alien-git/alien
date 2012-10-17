use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;

includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
my $firstId=shift;
my $secondId=shift;
my $guid=shift;


my $procDir=checkOutput($cat,$firstId) or exit(-2);

print "The output is $procDir\n";
my ($newguid)=$cat->execute('lfn2guid', "$procDir/myguidfile") or print "Error getting the guid from $procDir\n" and exit(-2);

$newguid=~ /^$guid$/i or print "The guid is different!!\n" and exit(-2);

print "ok!!\n";

print "\n\n\nIf we execute it again...\n";


my ($info)=$cat->execute("top", "-id", $secondId)  or exit(-2);
my ($user)=$cat->execute("whoami");
$procDir="~/alien-job-$secondId";
($info->{status} eq "ERROR_SV") or print "The job didn't finish in ERROR_SV but in: $info->{statusId}!!\n" and exit(-2);

print "The output should not be registered\n";

$cat->execute("ls",  "$procDir/myguidfile") and print "Error: the output of the job was registered!!!" and exit(-2);

print "ok!!\n";





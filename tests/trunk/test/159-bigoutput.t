use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);
includeTest("26-ProcessMonitorOutput") or exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user"=>"newuser"}) or exit(-2);


addFile($cat, "bin/bigOutput.sh","#!/bin/sh
echo \"This is creating a huge file\"
dd if=/dev/zero of=filename bs=4k count=1000
echo \"File created. Let's wait for one minute to see if the job gets killed\"
sleep 100
echo \"The job didn't get killed\"
") or exit(-2);


addFile($cat, "jdl/bigOutput.jdl", "Executable=\"bigOutput.sh\";
Workdirectorysize =  { \"2MB\" };
") or exit(-2);

addFile($cat, "jdl/bigOutputWorks.jdl", "Executable=\"bigOutput.sh\";
Workdirectorysize =  { \"10MB\" };
") or exit(-2);


my $procDir=executeJDLFile($cat, "jdl/bigOutput.jdl", "ERROR_E") or 
  print "The job was not killed \n" and exit(-2);


print "The job was killed!!\n";
print "And now, let's check with a job that is not supposed to be killed\n";
$procDir=executeJDLFile($cat, "jdl/bigOutputWorks.jdl") or 
  print "The job did not finish\n" and exit(-2);


print "OK!!\n";

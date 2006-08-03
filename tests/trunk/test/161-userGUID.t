use strict;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::GUID;

my $guid=AliEn::GUID->new()->CreateGuid();

print "HOLA $guid\n";
$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);
includeTest("26-ProcessMonitorOutput") or exit(-2);
my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);



addFile($cat, "bin/userGUIDS.sh","#!/bin/sh
echo \"Creating a file with a specific guid\"
echo \"myguidfile $guid\" >myguidfile
","r") or exit(-2);


addFile($cat, "jdl/userGUIDS.jdl", "Executable=\"userGUIDS.sh\";
GUIDFile=\"myguidfile\";
OutputFile=\"myguidfile\"
") or exit(-2);

my $procDir=executeJDLFile($cat, "jdl/userGUIDS.jdl", ) or 
  print "The job did not run\n" and exit(-2);

print "The output is $procDir\n";
my ($newguid)=$cat->execute('lfn2guid', "$procDir/job-output/myguidfile") or print "Error getting the guid from $procDir\n" and exit(-2);

$newguid=~ /^$guid$/i or print "The guid is different!!\n" and exit(-2);

print "ok!!\n";

print "\n\n\nIf we execute it again...\n";

$procDir=executeJDLFile($cat, "jdl/userGUIDS.jdl", ) or 
  print "The job did not run\n" and exit(-2);

print "The output should not be registered\n";

$cat->execute("ls",  "$procDir/job-output/myguidfile") and print "Error: the output of the job was registered!!!" and exit(-2);

print "ok!!\n";





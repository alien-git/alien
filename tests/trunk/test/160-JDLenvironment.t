use strict;

use AliEn::UI::Catalogue::LCM::Computer;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);
includeTest("26-ProcessMonitorOutput") or exit(-2);
my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);


addFile($cat, "bin/jdlEnvironment.sh","#!/bin/sh
echo \"This checks if some environment variables are created\"
echo \"ALIEN_JDL_MY_VARIABLE=\$ALIEN_JDL_MY_VARIABLE\"
") or exit(-2);


addFile($cat, "jdl/jdlEnvironment.jdl", "Executable=\"jdlEnvironment.sh\";
JDL_VARIABLES={\"MY_VARIABLE\"};
MY_VARIABLE=\"Hello world\"
") or exit(-2);

my $procDir=executeJDLFile($cat, "jdl/jdlEnvironment.jdl", ) or 
  print "The job was did not run\n" and exit(-2);

print "The output is $procDir\n";
my ($stdout)=$cat->execute('get', "$procDir/job-output/stdout") or print "Error getting the stdout of the job $procDir\n" and exit(-2);

open (FILE, "<$stdout") or print "Error opening the local file $stdout\n" and exit(-2);
my @content=<FILE>;
close FILE;
my $line= join ("", grep (/ALIEN_JDL_MY_VARIABLE/, @content));

$line or print "Error finding the ALIEN_JDL_MY_VARIABLE line!!\n" and exit(-2);

$line=~ /Hello world/ or print "The environment variable was not defined!!\n" and exit(-2);

print "ok!!\n";



use strict;

use AliEn::UI::Catalogue::LCM::Computer;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("catalogue/003-add") or exit(-2);
includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);


my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
my ($dir)=$cat->execute("pwd") or exit (-2);


addFile($cat, "bin/validate","#!/bin/bash
echo \"Cheking if the script said anything about beautiful days\"
grep \"It is a beautiful day\" stdout || exit -2;
echo \"Yep, it is a beautiful day\"
") or exit(-2);


addFile($cat, "jdl/validateJob.jdl","Executable=\"echo.sh\";
ValidationCommand=\"$dir/bin/validate\";
Arguments=\"It is a beautiful day\";
") or exit(-2);

addFile($cat, "jdl/validateJobFailed.jdl","Executable=\"echo.sh\";
ValidationCommand=\"$dir/bin/validate\";
Arguments=\"It is not a beautiful day\";
") or exit(-2);

my ($id)=$cat->execute("submit", "jdl/validateJob.jdl") or exit(-2);
my ($id2)=$cat->execute("submit", "jdl/validateJobFailed.jdl") or exit(-2);

print "We have submitted both jobs!!\n
\#ALIEN_OUTPUT $id $id2\n";


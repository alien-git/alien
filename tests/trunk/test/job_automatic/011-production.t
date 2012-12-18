use strict;

use AliEn::UI::Catalogue::LCM::Computer;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("catalogue/003-add") or exit(-2);
includeTest("job_automatic/008-split") or exit(-2);


my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
my ($dir)=$cat->execute("pwd") or exit (-2);

addFile($cat, "bin/production.sh","#!/bin/bash
date
echo 'We are executing a production!!'
echo \"I've been called with '\$*'\"
if [ \"\$1\" = \"2\" ];
then
   echo \"Let's simulate that this event fails\"
   echo \"Segmentation fault\"
   exit 2
fi
echo \"Event \$1 finished successfully\"
") or exit(-2);

addFile($cat, "bin/validateProduction","#!/bin/bash
echo \"Cheking if the script finished successfully\"
grep \"Segmentation fault\" stdout && exit -2;
grep \"Event .* finished successfully\" stdout || exit-2;
echo \"Validation passed!!\"
") or exit(-2);


my $outputDir="$dir/production";
$cat->execute("rmdir", "-rf", $outputDir);
addFile($cat, "jdl/production.jdl","Executable=\"production.sh\";
Split=\"production:1-5\";
SplitArguments=\"#alien_counter# big production\";
OutputDir=\"$outputDir/#alien_counter_03i#/\";
validationcommand=\"$dir/bin/validateProduction\";
","r") or exit(-2);
my ($id)=$cat->execute("submit", "jdl/production.jdl") or exit(-2);
$cat->close();
print "ok!!\n
\#ALIEN_OUTPUT $id\n";

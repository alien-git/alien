use strict;

use AliEn::UI::Catalogue::LCM::Computer;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);
includeTest("86-split") or exit(-2);


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
#$cat->execute("rmdir", "-rf", $outputDir);
addFile($cat, "jdl/production.jdl","Executable=\"production.sh\";
Split=\"production:1-5\";
SplitArguments=\"#alien_counter# big production\";
OutputDir=\"$outputDir/#alien_counter_03i#/\";
validationcommand=\"$dir/bin/validateProduction\";
","r") or exit(-2);
my ($ok, $procDir, $subjobs)=executeSplitJob($cat, "jdl/production.jdl",{noSubjobs=>1}) or exit(-2);

$subjobs eq "5" or print "The job is not split in 5 subjobs\n" and exit(-2);

print "Production executed\n";

print "Let's check that the output dir is ok\n";

my @entries=$cat->execute("ls", $outputDir) or exit(-2);

$#entries eq "3" or print "There are too many entries!! @entries\n" and exit(-2);
foreach (@entries) {
  print "\tChecking $_\n";
  my ($file)=$cat->execute("get", "$outputDir/$_/stdout", "-silent") 
    or print "Error getting $_\n" and exit(-2);
  system ("grep  'finished successfully' $file") and
    print "Error: event $_ didn't print anything\n" and exit(-2);
}


print "DONE!!\n";


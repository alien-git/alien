use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);
includeTest("26-ProcessMonitorOutput") or exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user"=>"newuser"}) or exit(-2);


$cat->execute("rmdir", "-rf", "zip", "-silent");
$cat->execute("mkdir", "zip");


addFile($cat, "zip/file1","Testing the zipping
") or exit(-2);
addFile($cat, "zip/file2", "Another file for the zip
") or exit(-2);

$cat->execute("cd","zip") or exit(-2);
$cat->execute("zip", "files.zip", "file1","file2") or exit(-2);
print "Zip files created!!!\n";
$cat->execute("cd","..") or exit(-2);

my $dir="/tmp/alien_test.157.$$";
mkdir $dir or print "Error creating $dir\n" and exit (-2);
chdir $dir;
my ($done)=$cat->execute("unzip", "zip/files.zip");
print "Files extracted!!!\n";
opendir (DIR, "$dir");
my @dirs=readdir(DIR);
closedir DIR;
print "GOT @dirs\n";

system("rm","-rf", $dir);
$done or print "The unzip returned an error!!\n" and exit(-2);

(grep (/^file1$/, @dirs) and  grep (/^file2$/, @dirs)) 
  or print "The files were not extracted!!\n" and exit -2;

print "ok\n";

addFile($cat, "jdl/InputZip.jdl","Executable=\"CheckInputOuptut.sh\";
InputZip=\"zip/files.zip\";
") or exit(-2);

my $procDir=executeJDLFile($cat, "jdl/InputZip.jdl")  or exit(-2);

print "JOB DONE ($procDir)!!\n\n";
my ($output)=$cat->execute("get", "$procDir/job-output/stdout", "-silent") or print "Error getting the output\n" and exit(-2);

  
  open (FILE, "<$output") or print "Error opening the file $output\n";
my @content=<FILE>;
close FILE;

print @content;

grep (/ file1$/, @content) or print "Error the file file1 is not there!!!!\n" and exit(-2);
grep (/ file2$/, @content) or print "Error the file file1 is not there!!!!\n" and exit(-2);

print "ok\n";

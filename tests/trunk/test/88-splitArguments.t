use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);


  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);

  addFile($cat, "jdl/SplitArgs.jdl","Executable=\"CheckInputOuptut.sh\";
Split=\"directory\";
SplitArguments={\" allfiles: '#alienallfulldir#' dir: '#aliendir#'\",
                 \" second round\"};
InputData=\"LF:${dir}split/*/*\";" ) or exit(-2);
  my @files=$cat->execute("find", "${dir}/split/", "*");
  print "Starting with @files\n";

  my ($id)=$cat->execute("submit", "jdl/SplitArgs.jdl") or exit(-2);
  $cat->close();
  print "ok!!\n
\#ALIEN_OUTPUT $id @files\n";
}

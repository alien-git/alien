use strict;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::GUID;

my $guid=AliEn::GUID->new()->CreateGuid();

print "HOLA $guid\n";
$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add") or exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);

addFile($cat, "bin/userGUIDS.sh","#!/bin/sh
echo \"Creating a file with a specific guid\"
echo \"myguidfile $guid\" >myguidfile
","r") or exit(-2);


addFile($cat, "jdl/userGUIDS.jdl", "Executable=\"userGUIDS.sh\";
GUIDFile=\"myguidfile\";
OutputFile={\"myguidfile\",\"stdout\",\"stderr\",\"resources\"};
") or exit(-2);

addFile($cat, "bin/userGUIDSFail.sh","#!/bin/sh
echo \"Creating a file with a specific guid (but after sleeping for a while)\"
sleep 60
echo \"myguidfile $guid\" >myguidfile
","r") or exit(-2);


addFile($cat, "jdl/userGUIDSFail.jdl", "Executable=\"userGUIDSFail.sh\";
GUIDFile=\"myguidfile\";
OutputFile={\"myguidfile\",\"stdout\",\"stderr\",\"resources\"};
") or exit(-2);

my ($id)=$cat->execute("submit", "jdl/userGUIDS.jdl") or exit(-2);

print "Submitting the second job\n";
my ($id2)=$cat->execute("submit", "jdl/userGUIDSFail.jdl") or exit(-2);
$cat->close();

print "Job submitted!! 
\#ALIEN_OUTPUT $id $id2 $guid\n";








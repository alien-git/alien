use ExtUtils::MakeMaker;
use Config;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'		=> 'AliEn::LCG',
    'VERSION_FROM'	=> 'lib/AliEn/LCG.pm', # finds $VERSION
    'PREREQ_PM'		=> {}, # e.g., Module::Name => 1.1
    'INSTALLSITESCRIPT'     => "$Config{prefix}/scripts/lcg",
    'INST_SCRIPT'       => "$Config{prefix}/scripts/lcg",
    'EXE_FILES'         => ['scripts/lcgAlien.sh', 'scripts/failoverSubmit.pl', 'scripts/checkJAStatus.pl'],
    'PERL'              =>"$Config{prefix}/bin/alien-perl",
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM => 'lib/AliEn/LCG.pm', # retrieve abstract from module
       AUTHOR     => 'Pablo Saiz <Pablo.Saiz@cern.ch>') : ()),
);


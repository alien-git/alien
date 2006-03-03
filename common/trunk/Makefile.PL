use ExtUtils::MakeMaker;
use Config;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'		=> 'AliEn',
    'VERSION_FROM'	=> 'lib/AliEn/Config.pm',
    'PREREQ_PM'		=> {}, # e.g., Module::Name => 1.1
    'PERL'              =>"$Config{prefix}/bin/alien-perl",
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM => 'lib/AliEn/Config.pm', # retrieve abstract from module
       AUTHOR     => 'Pablo Saiz <Pablo.Saiz@cern.ch>') : ()),
);


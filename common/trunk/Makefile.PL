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
	      'INSTALLSCRIPT'     => "$Config{prefix}/scripts",
	      'INSTALLSITESCRIPT' =>"$Config{prefix}/scripts",
	      
	      'EXE_FILES'         => [
				      'scripts/API.pl',
				      'scripts/Alien.sh',
				      'scripts/ApiConfig.pl',
				      'scripts/AlienServices.sh',
				      'scripts/Authen.pl',
				      'scripts/CreateMan.sh',
				      'scripts/Prompt.pl',
				      'scripts/ProxyServer.pl',
				      'scripts/Command.pl',
				      'scripts/GetConfigVar.pl',
				      'scripts/CE.pl',
				      'scripts/ProcessMonitor.pl',
				      'scripts/Service.pl',
				      'scripts/Update.pl',
				      'scripts/alien',
				      'scripts/amon',
				      'scripts/checkHosts.pl',
				      'scripts/createKeys.pl',
				      'scripts/date.pl',
				      'scripts/expungeProcesses.pl',
				      'scripts/updateToken.pl',
				      'scripts/updateCert.pl',
				      'scripts/requestCert.pl',
				      'scripts/requestCertificate.pl',
				      'scripts/proxyInit.pl',
				      'scripts/ModuleVersions.pl',
				      'scripts/SaveFile.pl',
				      'scripts/makewhatis',
				      'scripts/rotatelog.pl',
				      'scripts/Validate.pl',
				      'scripts/gContainer.pl',
                                      'scripts/pingService.pl',
                                      'scripts/ServiceStatus.sh']
	     );


package AliEn::PackMan::Local;

use AliEn::Config;
use strict;
use vars qw(@ISA);
use AliEn::UI::Catalogue::LCM;
use Filesys::DiskFree;
use AliEn::Util;
use AliEn::SOAP;
use Data::Dumper;
push @ISA, 'AliEn::Logger::LogObject', 'AliEn::PackMan';


sub initialize{
  my $self=shift;
  $self->{INST_DIR}=($self->{CONFIG}->{PACKMAN_INSTALLDIR} || "$ENV{ALIEN_HOME}/packages");
  
  $self->info("Using $self->{INST_DIR} as the installation directory");
  while ($self->{INST_DIR} =~  s/\$([^\/]*)/$ENV{$1}/ ) {
    $self->debug(1, "The installdir contains \$$1. Let's replace it with $ENV{$1}");
    $ENV{$1} or $self->info("Error: the environment variable $1 is not defined. Using the home directory to install the packages") and $self->{INST_DIR}= "$ENV{ALIEN_HOME}/packages";
  }
  if (! -d $self->{INST_DIR}) {
    $self->info( "$$ Creating the directory $self->{INST_DIR}");
    require  AliEn::MSS::file;;
    AliEn::MSS::file::mkdir($self, $self->{INST_DIR}) and return;
  }
  $self->info("CREATING THE CATALOGUE");
  $self->{CATALOGUE}=AliEn::UI::Catalogue::LCM->new({no_catalog=>1})  or return;


  return $self->SUPER::initialize();
}

sub removeLocks{
  my $self=shift;
  open (FILE, "ls $self->{INST_DIR}/*.InstallLock 2>/dev/null |") or
    $self->info("Error removing the locks");
  while (<FILE>){
    my $lock=$_;
    $self->info("Ready to remove $lock !");
    if ($lock =~ /^(.*)\.([^\.]*)\.([^\.]*).InstallLock$/){
      $self->info("Removing the directory  $1/$2/$3");
      system ("rm -rf $1/$2/$3");
    }
    system ("rm -f $lock");
  }
  close FILE;

}
sub getListInstalled_Internal {
  my $self=shift;
  $self->info("Checking the packages that we have installed locally");
  my @allPackages=();
  eval {
    my $dir="$self->{INST_DIR}";
    $self->debug(1, "Checking $dir");
    foreach my $user ($self->getSubDir($dir)) {
      $self->debug(1, "Checking $dir/$user");
      foreach my $package ($self->getSubDir("$dir/$user")){
	$self->debug(1, "Checking $dir/$user/$package");
	foreach my $version ($self->getSubDir("$dir/$user/$package")){
	  $self->debug(1, "Checking $dir/$user/$package/$version");
	  (-f "$dir/$user.$package.$version.InstallLock") and
	    $self->debug(1, "The package is being installed") and next;
	  push @allPackages, "${user}\@${package}::$version";
	}
      }
    }
    
  };
  if ($@) {
    $self->info( "$$ We couldn't find the packages ");
    die ($@);
  }

  return  1, @allPackages;

}

sub getSubDir{
  my $self=shift;
  my $dir=shift;
  opendir (DIR, $dir) or $self->info( "$$ Error reading $dir\n")
    and die("Error reading $dir");
  my @entries = grep { ( ! /^\./ ) && -d "$dir/$_" } readdir(DIR);
  closedir DIR;
  return @entries;
}

sub installPackage{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $dependencies=shift ||{};
  my $options=shift ||{};

  $self->debug (2,"checking if we have to install the package");
  my $source="";
  eval {
    my ($lfn, $info)=$self->findPackageLFN($user, $package, $version);
    
    $version or $lfn =~ /\/([^\/]*)\/[^\/]*$/
      and ($version)=($1);
    if ($lfn =~ m{^/$self->{CONFIG}->{ORG_NAME}/packages/}i ) {
      $self->info( "$$ This package is defined by the VO. Let's put the user to VO");
      $user=uc("VO_$self->{CONFIG}->{ORG_NAME}");
    }
    
    #First, let's try to install all the dependencies
    $self->info("Ready to install $package and $version and $user (from $lfn)");
    $dependencies->{"${package}::$version"}=1;
    
    my $old=$options->{NO_FORK} || 0;

    if ($info && $info->{dependencies}) {

      $self->info( "$$ Installing the dependencies of $package (without forking");
      $options->{NO_FORK}=1;
      foreach (split(/,/,$info->{dependencies})){
	my ($pack, $ver)=split(/::/, $_, 2);
	my $pack_user=$user;
	$pack=~ s/^(.*)\@// and $pack_user=$1;
	#let's install the packages without configuring them
	if ($dependencies->{"${pack}::$ver"} ) {
	  $self->info( "$$ Package $pack $ver already configured");
	  next;
	}
	
	my ($ok, $depsource, $dir)=$self->installPackage($user, $pack, $ver, $dependencies, $options);
	$depsource and $source="$source $depsource";
      }
    }
    $options->{NO_FORK}=$old;
    $self->debug(2,  "Ready to do the installPackage $package for $user");
    #Let's put the files public
    umask 0022;
    while (1){
      eval {
	$self->InstallPackage($lfn, $user, $package, $version,$info, $source, $options);
      };
      my $error=$@;
      if ($error) {
	if ($error =~ /Package is being installed/){
	  if ($self->{NO_FORK}){
	    $self->info("Let's sleep for a while...");
	    sleep(60);
	    next;
	  }
	}
	$self->info("I think that here I have to return '$error'...");
	die($error);
      }
      last;
    }
    umask 0027;
    my ($done, $psource, $dir2)= $self->ConfigurePackage($user, $package, $version, $info);
    $psource and $source="$source $psource";
    $self->info( "$$ Returning $done and ($source)\n");
  };
  my $error=$@;
  if ($error){
    $self->info("Error installing the package '$package'!! $error");
    return (-1, $error);
  }
  $self->info("Everything is ready. We just have to do $source");
  return (1, $source);
}


sub findPackageLFN{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  
  my $platform=AliEn::Util::getPlatform($self);
  $self->info("$$ Looking for the lfn of $package ($version) for the user $user");
  
  my $cacheName="lfn_${user}_${package}_${version}";
  my $cache=AliEn::Util::returnCacheValue($self, $cacheName);
  if ($cache) {
    $self->info("Returning from the cache $cacheName (@$cache)");
    return @$cache ;
  }

  my $result=$self->{SOAP}->CallSOAP("PackManMaster", "findPackageLFN", $user, $package, $version, $platform)
    or $self->info("Error talking to the PackManMaster") and return;

  my @info=$self->{SOAP}->GetOutput($result);
  if (  $info[0] eq /^-2$/ ){
    my $message="The package $package (v $version) does not exist for $platform \n";
    $self->info($message);
    die $message;
  }

  $self->info(Dumper(@info));
  if ( $info[1] =~ /^$/) {
    $self->info("The metadata is empty????");
    $info[1]={};
  }

  AliEn::Util::setCacheValue($self, $cacheName, \@info);

  return @info;
}


sub checkDiskSpace {
  my $self=shift;
  my $dir=shift;
  my $requestedSpace=shift;
  my $options=shift || {};

  my $handle=Filesys::DiskFree->new();
  $handle->df_dir($dir);
  my $size=$handle->avail($dir);
  #if there is enough space, just install
  if ($requestedSpace <$size){
    $self->debug(3, "There is enough space to install the file");
    return 1;
  }
  $options->{no_clean} and return;
  $self->info("Let's try with some clean up");
  my @packages=$self->findOldPackages();
  foreach my $package (@packages){
    $self->info("Lets's delete $package");
    $package=~ /^(.*)\@([^:]*)::(.*)$/ 
      or  $self->info("Error getting the info from $package") and next;
    my ($user, $package, $version)=($1,$2,$3);
    $self->removePackage($user, $package, $version);
  }
  $options->{no_clean}=1;
  return $self->checkDiskSpace($dir, $requestedSpace, $options);
}

sub InstallPackage {
  my $self=shift;
  my $lfn=shift;
  my ($user, $package, $version, $info,$depConf)=(shift, shift, shift,shift,shift);
  my $options=shift || {};

  my $dir="$self->{INST_DIR}/$user/$package/$version";
  my $lock="$self->{INST_DIR}/$user.$package.$version.InstallLock";
  my $logFile="$self->{INST_DIR}/$user.$package.$version.InstallLog";

  ( -f $lock) and $self->info( "$$ Package being installed (lock $lock)\n")
    and  die ("Package is being installed\n");


  $self->existsPackage($user, $package, $version,$info) and return 1;
  $self->info( "$$ Ready to install the package (output in $logFile) ");

  open FILE, ">$lock" 
    and close FILE
    or $self->info( "$$ Error creating $lock")
    and die ("Error creating $lock\n");
  system("mv",$logFile, "$logFile.back");
  $self->{LOGGER}->redirect($logFile);
  eval {
    if (! $info->{shared}) {
      AliEn::MSS::file::mkdir($self, $dir) and
	  $self->info( "$$ Error creating $dir") and
	    die("Error creating $dir\n");
      chdir $dir or die ("Error changing to $dir $!\n");
    } else {
      my $shared="$self->{INST_DIR}/$user/alien_shared";
      $self->info( "$$ This package has to be installed in a shared directory");
      AliEn::MSS::file::mkdir($self,$shared,"$self->{INST_DIR}/$user/$package/") and 
	  $self->info( "$$ Error creating the directory $shared") and die ("Error creating the directory $shared $!");
      system ("ln -s $shared $dir") and $self->info( "$$ Error creating the link") and die ("Error creating the link\n");
    }
    if (!$self->checkDiskSpace($dir, $info->{installedSize} )){
      die("Error checking for diskspace to install");
    }
  };
  if ($@) {
    system ("rm -rf $dir $lock");

    $self->info( "$$ Error $@");
    $self->{LOGGER}->redirect();
    $self->info( "$$ Error $@") and die ("Error $@\n");
  }


  $self->info( "$$ Installing package $package (V $version)");
  if (! $options->{NO_FORK}){
    my $pid=fork();
    if (!$pid){
      $self->info( "$$ Let's tell the client to retry in sometime...");
      $self->{LOGGER}->redirect();
      $self->info( "$$ Let's tell the client to retry in sometime...");
      die ("Package is being installed\n");
    }
  }

  eval {
    $self->_doAction($package, $version, $dir, $info, "pre_install", $depConf);
    $self->_Install($dir, $lfn, $info);
    $self->_doAction($package, $version, $dir, $info, "post_install", $depConf);

  };
  my $error=$@;

  system ("rm $lock");
  if ($error) {
    $self->{LOGGER}->redirect();
    system ("rm -rf $dir");
    $self->info( "$$ Error $@") and die ("Error $@\n");
  }
  $self->info( "$$ Package $package installed successfully!");
  $self->{LOGGER}->redirect();
  AliEn::Util::deleteCache($self);
  return 1;
}


sub _Install {
  my $self=shift;
  my $dir=shift;
  my $lfn=shift;
  my $metadata=shift;

   my ($file)=$self->{CATALOGUE}->execute("get", "-s", "no_se", $lfn)
      or die("getting the file $lfn\n");
  $self->info( "$$ Starting the uncompress...");
  system("tar zxf $file") and die("uncompressing $lfn ($file)\n");
  return 1;
}

sub ConfigurePackage{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $info=shift;

  $self->info( "$$ Configuring the package $package (v $version)");
  my $dir="$self->{INST_DIR}/$user/$package/$version";
  chdir $dir or 
    return $self->installPackage($user, $package, $version);
  my $sourceFile= ($info->{config} || ".alienEnvironment");
  $info->{path} and $dir.="/$info->{path}";

  my $source="";
  system("touch $dir/.alien_last_used");
  if (-f "$dir/$sourceFile"){
    $self->{LOGGER}->info( "$$ PacKMan","Testing if $dir/$sourceFile is executable");
    if (! -x "$dir/$sourceFile") {
      $self->info( "$$ The file wasn't executable");
      chmod 0750 ,"$dir/$sourceFile";
    }
    $source="$dir/$sourceFile $dir ";
  }
  return (1,$source);
}
sub existsPackage{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $info=shift;

  $self->debug(2, "Checking if $package is already installed");

  my $dir="$self->{INST_DIR}/$user/$package/$version";
  (-d $dir) or return;
  my $lock="$self->{INST_DIR}/$user.$package.$version.InstallLock";
  (-f $lock) and $self->info("The lock exists. Someone is installing the package") and return 
#  if (!-d $dir) {
#    $self->debug("Checking among the VO packages");
#    $dir="$self->{INST_DIR}/VO_\U$self->{CONFIG}->{ORG_NAME}\E/$package/$version";
#
#  } 

  $self->debug(2,  "$$ Checking the size of $dir");
  my $size="";

  if (-l $dir) {
    $self->info( "$$ This is installed in the common area... let's ignore it for the time being");
  }else {
#    open (FILE, "du -s $dir|") or 
#      $self->info( "$$ Error getting the size of the directory")
#	and return;
#    $size=<FILE>;
#    close FILE;
#    $size=~ s/^\s*(\d+)\s+.*$/$1/s;
#    if ( $size eq "0") {
#      $self->info( "$$ The size of the package is 0");
#      system("rm -rf $dir");
#    return;
#    }
  }
  $info and $info->{size} and chomp $info->{size};
  $info->{size} or $info->{size}="";
  $self->debug(2,  "$$ Size $size (has to be $info->{size})");
  if (  $info->{size} and ($size ne $info->{size}) ){
    $self->info( "$$ The size of the package does not correspond (has to be $info->{size} and is $size)");
    system("rm -rf $dir");
    return;
  }
  if ($info->{md5sum}) {
    $self->info( "$$ Checking the md5sum of $info->{executable}");
    chdir $dir;
    system("md5sum -c .alienmd5sum") and
      $self->info( "$$ Error checking the md5sumlist")
	and return;
  }
  $self->info( "The package is already installed (in $dir)!!");
  return $dir;
}



sub _doAction {
  my $self=shift;
  my ($package, $version)=(shift, shift);
  my $dir=shift;
  my $metadata=shift;
  my $action=shift;
  my $depConf=shift ||"";
  my $script=$metadata->{$action} or return 1;
  $self->info( "$$ Doing the $action with $script");
  my ($file)=$self->{CATALOGUE}->execute("get", $script)
    or die("Error getting the file $script for the $action\n");
  chmod(0750, $file);

#  open SAVEOUT,  ">&STDOUT";
#  open SAVEOUT2, ">&STDERR";

#  open SAVEOUT,  ">&STDOUT";
#  open SAVEOUT2, ">&STDERR";
#  my $log="$self->{CONFIG}->{LOG_DIR}/packman/$package.$version.$action.$self->{CONFIG}->{HOST}";
#  $self->{LOGGER}->redirect($log);
  

#  require AliEn::MSS::File;
#  AliEn::MSS::file::mkdir($self, "$self->{CONFIG}->{LOG_DIR}/packman");
#  if ( !open STDOUT, ">$log" ) {
#    open STDOUT, ">&SAVEOUT";
#    die "stdout not opened!!";
#  }
#  open( STDERR, ">&STDOUT" ) or die ("Error opening STDERR");
  my @todo=($file, $dir);
  $depConf =~ s{^\s*}{};
  $depConf =~ s{\s*$}{};

  $depConf and @todo=(split(/ /, $depConf), $file, $dir);
  $self->info( "$$ Calling '@todo'");

  my $error=system(@todo);
#  close STDOUT;

#  open STDOUT, ">&SAVEOUT";
#  open STDERR, ">&SAVEOUT2";

#  $self->{LOGGER}->redirect();
  $self->info( "$$ $action done with $error!!");
  $error and die("Error doing the $action!!\n");
  return 1;
}

sub testPackage{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $versionUser=shift;
  my $command=join(" ", @_);
  $command or $command="env";
  my ($done, $lfn, $info, $version)=$self->isPackageInstalled($user,$package,$versionUser);

  $done or  return (-1, "Package is not installed");
  $self->info( "$$ Ok, the package is installed");
  ($done, my $source, my $installDir)= 
    $self->installPackage($user, $package, $version);
  $self->info( "$$ We should do $done and $source");
  my $env="";
  if ($source) {
     $self->info( "$$ Lets's call $source $command");
    $env=`$source $command`;
    my $env2=`$source echo "The AliEn Package $package is installed properly"`;
    if ($env2 !~ /The AliEn Package $package is installed properly/s ){
      $self->info( "$$ Warning!!! The package has to source $source, but this script doesn't seem to execute commands");
      $env.="================================================
Warning!!!!
The package is supposed to do: $source
This script will receive as the first argument the directory where the package is installed, and then the command to execute. Please, make sure that the script finishes with a line that calls the rest of the arguments (something like \$*).";
    }
  }
  my $dir=$installDir || ".";
  my $md5file="$dir/.alien_md5_list";
  $self->info("And let's check the md5 sum");
  if (not -f "$md5file.sorted"){
    if (not -f "$md5file.lock"){
      system("touch $md5file.lock");
      system("rm", "-rf", $md5file);
      system("find", "$dir -type f -not -path '/.*' --exec md5sum {} >> $md5file \\;");
      system("sort $md5file > $md5file.sorted");
      system("md5sum $md5file.sorted > $md5file.total");
    }
  }
  if (-f "$md5file.total"){
    if(open (FILE, "<$md5file.total")){
      $env=join("", "And the total md5 is:", <FILE>, $env);
      close FILE;
    }
  }
  my $directory=`ls -la $installDir`;
  return ($version, $info, $directory, $env);
}

sub isPackageInstalled {
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;

  my ($lfn, $info)=$self->findPackageLFN($user, $package, $version);

  $version or $lfn =~ /\/([^\/]*)\/[^\/]*$/
    and ($version)=($1);

  my $vo=$self->{CONFIG}->{ORG_NAME};
  $lfn =~ m{^/$vo/packages/}i and $user=uc("VO_$vo");

  $self->existsPackage($user, $package, $version,$info)  or
    $self->info( "$$ The package is not installed :D") and 
      return ;
  return (1, $lfn, $info, $version)
}




sub findOldPackages {
  my $self=shift;
  open (FILES, "find  -name .alien_last_used|") or 
    $self->info("Error doing the find") and return;
  my @files=<FILES>;
  close FILES;
  my @list;
  my $now=time;
  foreach my $file (@files){
    chomp $file;
    $self->info("Checking if the package '$file' is old");
    my (@info)=stat $file;
    
    print "GOT @info (comparing $now and $info[9]\n";
    if  ($info[9]+3600*24*7 <$now) {
      print "The file $file hasn't been accessed in one week\n";
      $file =~ m{/([^/]*)/([^/]*)/([^/]*)/\.alien_last_used$} or 
	print "Error getting the information out of the link\n" and next;
      my ($user, $package, $version)=($1,$2,$3);

      push @list, "$user\@${package}::$version";
    }
  }
  $self->info("Returning @list");
  return @list;
}

sub removePackage{
  my $self=shift;
  $self->info( "$$ Removing the package @_");
  my $user=shift;
  my $package=shift;
  my $versionUser=shift;

  my ($done, $lfn, $info, $version)=$self->isPackageInstalled($user,$package,$versionUser);
  
  $done or return (-1, "Package is not installed");

  my $dir="$self->{INST_DIR}/$user/$package/$version";
  if (($dir=~ /\.\./) or ($dir=~ /\s/)) {
    $self->info( "$$ Error: someone is trying to delete another directory '$dir'");
    die("Error trying to delete $dir: this is not the directory where the package is installed\n");
  }

  system ("rm","-rf","$dir") and 
    $self->info( "$$ Error deleting the package")
      and die("Error deleting the directory $dir\n");
  $self->info( "$$ Package $package ($version) removed");
  AliEn::Util::deleteCache($self);
  return 1;
}

sub recomputeListPackages{
  my $self=shift;

  $self->info("Asking the package manager to recompute the list of packages");

  my $result=$self->{SOAP}->CallSOAP("PackManMaster", "recomputeListPackages")
    or $self->info("Error talking to the PackManMaster") and return;
  $self->info("The information will be updated in 20 seconds");
  return 1;
}
return 1;


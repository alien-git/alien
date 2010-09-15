package AliEn::PackMan::Local::afs;

use AliEn::Config;
use strict;
use vars qw(@ISA);
use AliEn::UI::Catalogue::LCM;
use Filesys::DiskFree;
use AliEn::Util;
use AliEn::PackMan::Local;
use AliEn::SOAP;
use Data::Dumper;
push @ISA, 'AliEn::Logger::LogObject', 'AliEn::PackMan::Local';

sub installPackage{
    
    my $self=shift;
    
    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};
    
    $self->info("the value of the dir is BEFORE INSTALLING: $self->{INST_DIR}"); 

    my @info= $self->SUPER::installPackage(@_) or return;


    $self->info("the value of the dir is AFTER INSTALLING: $self->{INST_DIR}");     

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;
    
# In the case of AFS area at CERN
    
    # Trigger the synchronization tool
    
    my $volumeread = "q.gd.alice.readonly";
    my $volumewrite = "q.gd.alice";
    
    $self->info("The volume to be synchronized at CERN is: $volumeread");
    
#    my @cmd = ("arc","-h afsdb1 -n jobexit alicesgm");

    system ("arc -h afsdb1 -n jobexit alicesgm");
#    if (system(@cmd)) {
     if ($?){
	$self->info ("The synchronization of the AFS volumes has not worked! ");
	return -1, "The synchronization failed";    
    }
    # Checking if the volumes are really synchronized

    my $counter = 0;
    for ($counter=0;$counter<10;$counter++){

	print "value of the counter!!!!!!!!!!!!!!: $counter\n";

	sleep (20);
	# There can be SEVERAL readonly modules, but ONE writable module
	my @synch1 = `/usr/sbin/vos examine $volumeread | grep Update`;
	my $synch2 = `/usr/sbin/vos examine $volumewrite | grep Update`;
	
	my @unique = ();
	my %seen = ();
	foreach my $elem ( @synch1 )
	{
	    next if $seen{ $elem }++;
	    push @unique, $elem;
	}
	
# The following variable must be 1: Otherwise the readonly modules are not synchronized
	my $check_tmp = @unique;
	
	if ($check_tmp != 1){
	    $self->info ("The readonly volumes are not synchronized! ");
	    return -1, "The readonly volumes are not synchronized!";
	}
# Comparing the strings

	print "MOREEEEEEEEEEEEE: $unique[0] \t $synch2\n";

     	if ($unique[0] eq $synch2){
	    $self->info("Great the 2 AFS modules are synchronized");
	    last;
	}
        else{
	    
            $self->("not yet synchronized, let's try it again in 20 seconds $counter time out of 10 times");	    
	}
	
    }
    return @info;
}

sub existsPackage{

    my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::existsPackage(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub InstallPackage{

    my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::InstallPackage(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}



sub createLock{

   my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::createLock(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;


}

sub ConfigurePackage{
    my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::ConfigurePackage(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;

}

sub removeLock{

    my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::removeLock(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;

}

sub removeLocks{

    my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::removeLocks(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}
### Adding the rest of the Local.pm packages

sub getListInstalled_Internal{
    my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::getListInstalled_Internal(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub getSubDir{
    my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::getSubDir(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub removeLock {
     my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::removeLock(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub recomputeListPackages{
   my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::recomputeListPackages(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub findPackageLFN{
    my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::findPackageLFN(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub findOldPackages {
  my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::findOldPackages(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub removePackage{
   my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::removePackage(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub _Install {
   my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::_Install(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub _doAction {
   my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::_doAction(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub isPackageInstalled {
    my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::isPackageInstalled(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub checkDiskSpace{
     my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::checkDiskSpace(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub ConfigurePackage{
   my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::ConfigurePackage(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub existsPackage{
   my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::existsPackage(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;
}

sub testPackage{
   my $self=shift;

    $self->{INST_DIR} =~s{/afs/cern.ch}{/afs/.cern.ch};

    my @info= $self->SUPER::testPackage(@_) or return;

    $self->{INST_DIR} =~s{/afs/.cern.ch}{/afs/cern.ch};
    ($info[0] == '-1') and  return @info;

   return @info;

}


return 1;


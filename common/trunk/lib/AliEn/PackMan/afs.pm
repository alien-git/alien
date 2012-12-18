package AliEn::PackMan::afs;

use AliEn::Config;
use strict;
use vars qw(@ISA);
use AliEn::UI::Catalogue::LCM;
use Filesys::DiskFree;
use AliEn::Util;
use AliEn::PackMan;

use Data::Dumper;
push @ISA, 'AliEn::Logger::LogObject', 'AliEn::PackMan';


sub initialize {
  my $self=shift;

  ($self->{INST_DIR}) or 
    $self->{INST_DIR}=($self->{CONFIG}->{PACKMAN_INSTALLDIR} || "$ENV{ALIEN_HOME}/packages");

  $self->{REALLY_INST_DIR}=$self->{INST_DIR};
  $self->{REALLY_INST_DIR}=~ s{^/afs/cern.ch}{/afs/.cern.ch}
    or $self->info("Warning! This packman was supposed to write to afs, but the installation directory is not afs ($self->{INST_DIR})") and return $self;
 
  $self->info("This packman puts the packages in a write afs. Then, it synchronizes");
  $self->SUPER::initialize or return;

  $self->synchronizeAFS();
  return $self;
}

sub installPackage{
  my $self=shift;

  my @info= $self->SUPER::installPackage(@_) or return;
  ($info[0] == '-1') and  return @info;
    
  # $self->synchronizeAFS();
  return @info;

}
sub synchronizeAFS {
  my $self=shift;
  # In the case of AFS area at CERN
  $self->info("Synchronizing the afs area");
  # Trigger the synchronization tool

  my $volumeread = "q.gd.alice.readonly";
  my $volumewrite = "q.gd.alice";

  $self->info("The volume to be synchronized at CERN is: $volumeread");

#    my @cmd = ("arc","-h afsdb1 -n jobexit alicesgm");

  system ("unset LD_LIBRARY_PATH; arc -h afsdb1 -n jobexit alicesgm");    

#    system ("arc -h afsdb1 -n jobexit alicesgm");
#    if (system(@cmd)) {
  if ($?){
    $self->info ("The synchronization of the AFS volumes has not worked! ");
    return -1, "The synchronization failed";    
  }
    # Checking if the volumes are really synchronized

  my $counter = 0;
  for ($counter=0;$counter<10;$counter++){

    $self->info("value of the counter!!!!!!!!!!!!!!: $counter");

    # There can be SEVERAL readonly modules, but ONE writable module
    my @synch1 = `/usr/sbin/vos examine $volumeread | grep Update`;
    my $synch2 = `/usr/sbin/vos examine $volumewrite | grep Update`;
	
    my @unique = ();
    my %seen = ();
    foreach my $elem ( @synch1 )  {
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

    $self->info("Comparing  $unique[0] \t $synch2");

    if ($unique[0] eq $synch2){
      $self->info("Great the 2 AFS modules are synchronized");
      last;
    }
    $self->("not yet synchronized, let's try it again in 20 seconds $counter time out of 10 times");	    
    sleep (20);

  }
  $self->info("Afs area syncrhonized");
  return 1;
}


sub removeLock{
  my $self=shift;
  $self->synchronizeAFS();
  return $self->SUPER::removeLock(@_);
}




sub removePackage{
  my $self=shift;
  my @info= $self->SUPER::removePackage(@_) or return;

  $self->synchronizeAFS();
  return @info;
}



return 1;


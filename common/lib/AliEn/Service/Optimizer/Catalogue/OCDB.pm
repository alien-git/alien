package AliEn::Service::Optimizer::Catalogue::OCDB;
 
use strict;
use Data::Dumper;

require AliEn::Service::Optimizer::Catalogue;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;
  $self->$method(@info, "The OCDB optimizer starts");
  $self->{SLEEP_PERIOD}=10*60;
  
  $self->{SIZE_LIMIT} = 314572800; #209715200;
  
  #Cleaning entries that fail too much
  eval {
    $self->{DB}->{LFN_DB}->do("insert into OCDB_FAILED select * from OCDB where failed >= 5");
    $self->{DB}->{LFN_DB}->do("delete from OCDB where failed >= 5");
    $self->insertOCDBIntoCVMFS(0);
    $self->insertOCDBIntoCVMFS(1);
  };
  if($@){
    $self->info("Died with: $@");	
  }
  
  $self->info("Going back to sleep");
  return;
}

sub insertOCDBIntoCVMFS {
	my $self = shift;
	my $ocdbType = shift;
	
	my $cvmfsPath = "/cvmfs/alice-ocdb.cern.ch/calibration/";
    my $lfnReq = "lfn like '/alice/data%'";
    $ocdbType and $lfnReq = "lfn like '/alice/simulation%'" 
              and $cvmfsPath = "/cvmfs/alice-ocdb.cern.ch/calibration/MC/";

    # We limit the objects per loop, including the ones failed 5h or more ago
    my $query = "SELECT entryId, lfn from OCDB where $lfnReq and (failed=0 or failed>0 and timestampdiff(SECOND,lastupdated,now())>=18000 ) limit 50";
              
	my $lfnIds = $self->{DB}->{LFN_DB}->query($query);
	scalar(@$lfnIds) or $self->info("Nothing to do $ocdbType") and return 1;

	my $time = time;
	my $dir = "/tmp/ocdb_${ocdbType}_$time";
	system("mkdir -p $dir > /dev/null 2>&1") 
	  and $self->info("Failed to create temporary directory ($dir)") and return;
	chdir $dir 
	  or $self->info("Unable to change directory ($dir)") and return;
	
#	open FILE, ">>", $self->{DIRFILES}."ocdb_${ocdbType}_".$time;
	
	my @okLfns;
	
	my $total_size=0;
		
	foreach my $lfn (@$lfnIds){
      $self->info("Adding $lfn->{lfn}");
      # Get the file and paths/names we need
      my ($localfile) = $self->{CATALOGUE}->execute("get", "-silent", "-x", $lfn->{lfn});
      
      $self->info("We got $localfile");
      
      if(!$localfile) {
      	$self->info("There was a problem getting the file $lfn->{lfn}, increasing failed counter");
      	$self->{DB}->{LFN_DB}->do("update OCDB set failed=failed+1 where entryId=?",{bind_values=>[$lfn->{entryId}]});
      	next;
      }
      
      my $size = -s $localfile;
      my $total = $total_size+$size;
      
      $total > $self->{SIZE_LIMIT} and $self->info("Last file ($size) passed the limit ($self->{SIZE_LIMIT}) to $total") and last;
      
      my ($file)  = $self->{CATALOGUE}->{CATALOG}->f_basename($lfn->{lfn});
      $lfn->{lfn} =~ s/$file//;     
      $ocdbType and $lfn->{lfn} =~ s/^\/alice\/simulation\/2008\/v4-15-Release\/// 
                or  $lfn->{lfn} =~ s/^\/alice\///;
      
      $self->info("We have lfn $lfn->{lfn}$file - size $size - dir $dir - ocdbType $ocdbType");
      
      # Create the folders and move the files inside
      system("mkdir -p $lfn->{lfn} > /dev/null 2>&1") 
        and $self->info("Failed to create directory ($lfn->{lfn})") and next;
      system("mv $localfile $lfn->{lfn}$file")
        and $self->info("Failed to move file ($localfile to $lfn->{lfn}$file)") and system("rm -rf $localfile") and next;
      
      push @okLfns, $lfn->{entryId};
      $total_size = $total;
#      print FILE "$lfn->{lfn}$file \n";
	}
	
	$self->info("Total size: $total_size");
    
    my $error = 0;
    
    scalar(@okLfns) or $self->info("No lfns processed succesfully :(") and $error=1;
    
    # making sure permissions are ok
    system("find $dir -type d -exec chmod 755 {} +") 
      and $self->info("Failed putting 755 to folders") and $error = 1;
	system("find $dir -type f -exec chmod 644 {} +") 
	  and $self->info("Failed putting 644 to files") and $error = 1;
    
    $error or (system("tar --transform 's,^,".$cvmfsPath.",S' -cvzf $dir.tar.gz *")
      and $self->info("Failed creating tarball to upload to CVMFS") and $error = 1);

#	$error and print FILE "FAILED \n";
#    close FILE;
    
    my $command = "ocdb-cvmfs $dir.tar.gz ".( $ENV{OCDB_NOTIFICATION_EMAIL} ? "--mailto $ENV{OCDB_NOTIFICATION_EMAIL}" : "" )." 2>&1";
    
    $self->info("Calling $command");
    
    my $failed="";
    if (!$error) { 
    	$failed=`$command`; 
        $self->info(Dumper($failed)); 
        $failed ne "" and $self->info("OCDB CVMFS script failed") and $error=2; 
    }
    
    # delete from the table
    $error or $self->{DB}->{LFN_DB}->do("delete from OCDB where entryId in (".join(',', map { '?' } @okLfns).")", {bind_values => [@okLfns]});
    
    system("rm -f $dir.tar.gz");
    system("rm -rf $dir");
    chdir "/tmp";
    
    $error==2 and $self->{SLEEP_PERIOD}=120; # we wait 2 minutes to retry in case of failed publication
			
    return 1;
}

return 1;


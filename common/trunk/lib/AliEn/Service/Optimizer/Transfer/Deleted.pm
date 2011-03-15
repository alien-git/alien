package AliEn::Service::Optimizer::Transfer::Deleted;

use strict;

use vars qw (@ISA);
use AliEn::Service::Optimizer::Transfer;

push (@ISA, "AliEn::Service::Optimizer::Transfer");

sub checkWakesUp {
  my $self=shift;
  my $silent=(shift or 0);
  my $method="info";
  my @silentData=();
  $silent and $method="debug" and push @silentData, 1;
  $self->{SLEEP_PERIOD}=10;
  $self->$method(@silentData, "Checking if there is anything to do");

  my $rhosts = $self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->getAllHosts();

  foreach my $rtempHost (@$rhosts) {
    my ($db2, $extra)=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->reconnectToIndex( $rtempHost->{hostIndex}, "", $rtempHost );
    $db2 or $self->info("Error reconecting to $rtempHost->{hostIndex}") and next;
    my $query = "select entryId,pfn,sename from TODELETE join SE using (senumber) order by entryId";
    $query = $db2->paginate($query,100,0);
    my $d=$db2->query($query);
    #my $d=$db2->query("select entryId,pfn,sename from TODELETE join SE using (senumber) order by entryId limit 100");
    my $max=0;
    foreach my $entry (@$d){
      $self->info("Inserting a new request to delete");
      $entry->{entryId}> $max and $max=$entry->{entryId};
      my @protocols=$self->findDeleteProtocol($entry->{sename});
      my $fullProt='"'.join('","', @protocols).'"';
      map {$_=~ s/^(.*)$/member\(other\.SupportedProtocol, "$1" \)/ } @protocols;
      
      my $value=join("||",@protocols);
      my $jdl=$self->createJDL({Type=>'"transfer"', pfn=>"\"$entry->{pfn}\"", 
				destination=>"\"$entry->{sename}\"",
				Requirements=>"(other.type==\"FTD\")&&($value)",
				FullProtocolList=>"{$fullProt}",
				Action=>'"remove"',
			       }) 
	or $self->info("Error creating the jdl") and next;
      my $id=$self->{DB}->insertTransferLocked({status=>'TODELETE', 
						destination=>$entry->{sename},
						pfn=>$entry->{pfn},user=>'admin',
						lfn=>'',attempts=>'0'
					       }
					      );
      $id or $self->info("Error inserting the transfer") and next;
      #We do it in two steps to have the jobagent as well
      $self->{DB}->updateTransfer($id, {status=>'WAITING', jdl=>$jdl});
      $self->{TRANSFERLOG}->putlog($id,"STATUS", "Request to delete the pfn $entry->{pfn}");
    }
    if ($max){
      $db2->do ("DELETE FROM TODELETE where entryId<=?", {bind_values=>[$max]});
    }
  }
  return;
}

sub findDeleteProtocol{
  my $self=shift;
  my $sename=shift;
  my $items=$self->{DB}->queryColumn("SELECT protocol from PROTOCOLS where sename=? and deleteprotocol=1", undef ,{bind_values=>[$sename]});
  my @list=@$items;
  @list or $self->info("There are no default method to remove. Assuming rm") and push @list, "rm";
  return @list;
}

return 1;

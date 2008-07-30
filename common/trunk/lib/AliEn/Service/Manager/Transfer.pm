package AliEn::Service::Manager::Transfer;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Database::Transfer;

use AliEn::Service::Manager;

use strict;

use Classad;

use vars qw (@ISA);
@ISA=("AliEn::Service::Manager");

my $self = {};

sub initialize {
  $self     = shift;
  my $options =(shift or {});

  $self->debug(1, "In initialize initializing service TransferManager" );

  $self->{SERVICE}="Transfer";

  $self->{DB_MODULE}="AliEn::Database::Transfer";
    
  return $self->SUPER::initialize($options);
}


sub enterTransfer {
	my $this = shift;
	$self->debug(1, "In enterTransfer asking for a new transfer @_" );
	my $arguments=shift;
	
	my $message="";
	$arguments or $message="not enough arguments ";
	$arguments and (! UNIVERSAL::isa( $arguments, "HASH" ))
	and $message="arguments is not a hash";
	
	$arguments or $arguments={};
	my ($user, $lfn, $destination, $options, $type, $pfn)=
	($arguments->{USER}, $arguments->{LFN}, $arguments->{DESTINATION}, $arguments->{OPTIONS}, $arguments->{TYPE}, $arguments->{TOPFN}); 
	
	if (! $message){
		$user or $message="missing USER ";
		$lfn  or $message.="missing LFN ";
		$destination or $message.="missing DESTINATION ";
		$type or $type="cache";
		$pfn or $pfn="";
	}
	
	$message and $self->{LOGGER}->error("TransferManager", "In enterTransfer error: $message")
	and return (-1, $message);
	
	my $priority = ( $arguments->{PRIORITY}  or 0 );
	
	my $date = time;
	
	$self->info( "Checking if the transfer is already scheduled");
	
	#emir:
	my $tranID;
	$tranID = $self->{DB}->isScheduled($lfn,$destination) and
	$self->info( "Transfer has already been scheduled (transferid: $tranID)")
	and return $tranID;
	
	$self->info( "Entering a new transfer" );
	
	#emir:  
	my $info={received=>$date, user=>$user, lfn=>$lfn, pfn=>$pfn,
		destination=>$destination, type=>$type, options=>$options};
	$arguments->{transferGroup} and $info->{transferGroup}=$arguments->{transferGroup};
	$arguments->{collection} and $info->{collection}=$arguments->{collection};
	$arguments->{persevere} and $info->{persevere}=$arguments->{persevere};
	if($info->{persevere}<0){
		$self->{LOGGER}->warning("Number of tries must be greater than 0. Setting tries to 0");
		$info->{persevere} = 0;
	}
	$info->{attempts}=0;
	
	my $procid = $self->{DB}->insertTransferLocked($info) or
	$self->{LOGGER}->error( "TransferManager", "In enterTransfer insertion of a new transfer failed" )
	and return (-1, "in enterTransfer inserting a new transfer");
	
# 	$self->info( "New transfer inserted $procid" );
		
	return $procid;
}

sub changeStatusTransfer {
	my $this=shift;
	my $id=shift;
	my $status=shift;
	my $options=(shift or {});
	
	$self->debug(1, "In changeStatusTransfer changing the status of a transfer");
	
	my $message="";
	$id or $message="no id specified ";
	$status or $message.="no $status specified";
	
	$message and $self->{LOGGER}->error("TransferManager", "In changeStatusTransfer error: $message")
	and return (-1 , $message);
	
	$self->info( "Changing the status of transfer $id to $status with options $options");
	
	my ($done, $newJDL)=$self->getNewRequirements($status, $id, $options);
	
		
	if (!$done){
		$status=~  /^KILLED/ or 
		return (-1, "In changeStatusTransfer getting the new requirements");
		$self->info("Ok, we just want to kill it...");
		$newJDL="";
	}
		# temporary solution!!!
		#my @tmpArr = split($newJDL,"=");
	
		#$newJDL = $tmpArr[1];
	
	my $query= {status=>$status, SE=>undef, jdl=>$newJDL};
	
	my $date=time;
	
	if($status eq "TRANSFERING"){
		$query->{started} = $date;
		my $info = $self->{DB}->query("select size, destination from TRANSFERS where TRANSFERID=?", undef, {bind_values=>[$id]});
		if($info && @{$info}){
			$query->{size} = ${$info}[0]->{size};
			$query->{destination} = ${$info}[0]->{destination};
		}
	}
	
	($status eq "CLEANING" or $status eq "FAILED") and $query->{finished} = $date;
	
	$options->{FinalPFN}
		and $self->info( "Setting FinalPFN") and $query->{pfn} = $options->{FinalPFN};
	
	$done=$self->{DB}->updateTransfer($id, $query);
	
	$done or  $self->{LOGGER}->error("TransferManager", "In changeStatusTransfer error: Updating the status")
		and return (-1 , "updating the status");
	
	$self->info( "Change done");
	
	if($status eq "DONE"){
		$self->updateCatalogue($id);
	}
	
	if ($status =~ /^(DONE)|(FAILED)|(KILLED)$/){
		my $father=$self->{DB}->queryValue("SELECT transferGroup from TRANSFERS where transferId=?", undef, {bind_values=>[$id]});
		if ($father){
			$self->info("This is a final status. Updating the father");
			$father.=",";
			$self->{DB}->do("update ACTIONS set todo=1, extra=concat(replace(extra,?, ''), ?)where action='MERGING'", {bind_values=>[$father, $father]});
		}
	}
	
	
	#check if the DB has an attempts field
	my $attempts;
	$attempts=$self->{DB}->queryValue("SELECT attempts from TRANSFERS where transferid=$id") or $attempts=0;  
	
	
	#This doesn't work cause some other module changes the jdl
# 	my $ca=Classad::Classad->new($newJDL);
# 	my ( $ok, $attempts)=$ca->evaluateAttributeInt("FailedTransferAttempts");  
	# if not crate it
# 	if(!$ok){
# 		$self->info( "FailedTransferAttempts was missing !");
# 		$ca->insertAttributeInt("FailedTransferAttempts", 0 )
# 		or $self->{LOGGER}->error("changeStatusTransfer:", "Error putting FailedTransferAttempts Attribute in JDL for Transfer $id");
# 	}
# 	$newJDL = $ca->asJDL();

	#check if the DB has a persevere entry, if not set defult value to 5
	my $persevere;
	$persevere=$self->{DB}->queryValue("SELECT persevere from TRANSFERS where transferid=$id") or $persevere=5;  
	
	#if it tryed more than persevere give it up
	if($status eq "FAILED" and $attempts>=$persevere){
		$self->info( "Giving up Transfer $id tried $attempts / $persevere times");
		$query->{status}=$status;
		$query->{attempts}=$attempts;
	} 
	
	#if it failed try aging 
	if($status eq "FAILED" and $attempts<$persevere){
		$attempts++;
# 		$ca->insertAttributeInt("FailedTransferAttempts", $attempts);
# 		$newJDL = $ca->asJDL();
		$status = "INSERTING";
		$self->info( "Transfer $id Failed. Retrying. Attempt num $attempts / $persevere");
		$self->resubmitTransfer($id);		
# 		$query->{jdl}=$newJDL;
		$query->{status}=$status;
		$query->{attempts}=$attempts;
		$self->{DB}->updateTransfer($id, $query)
			or $self->{LOGGER}->error("TransferManager","In changeStatusTransfer error: Failed to update the datbase\n");
	}  

	
# 	my $tmp=$self->{DB}->queryValue("SELECT status from TRANSFERS where transferid=$id");
# 	$self->info("changeStatusTransfer: Last transfer status....$tmp");
	
# 	$tmp=$self->{DB}->queryValue("SELECT attempts from TRANSFERS where transferid=$id");
# 	$self->info("changeStatusTransfer: attempts....$tmp");	
	
	return 1;
}

sub updateCatalogue {
  my $self=shift;
  my $id=shift;


  $self->debug(1,"In updateCatalogue checking if we have to update the catalogue");

  my ($data)=$self->{DB}->getFields($id,"type,user,lfn,pfn,destination,jdl");

  defined $data
    or $self->{LOGGER}->error("JobManager","In updateCatalogue error during execution of database query")
      and return (-1, "error during execution of database query");

  $data->{type} or
    $self->info( "Transfer $id has no type")
      and return 1;

  $self->info( "Transfer $id wants to $data->{type}");
  my @command=();
  if ($data->{type} =~ /(mirror)|(master)$/) {
    $self->debug(1, "In updateCatalogue adding a mirror");
    @command=("addMirror", $data->{lfn}, $data->{destination}, $data->{pfn});
  } elsif ($data->{type} eq "cache") {
    $self->debug(1, "In updateCatalogue just making a cache copy");

    $self->info( "It was just a cache copy");
#     my $done=$self->{SOAP}->CallSOAP("IS", "getService", $data->{destination}, "SE");
#    if ($self->{SOAP}->checkSOAPreturn($done)){
#      $self->info("The call to the IS worked");
#      my $SOAPdata=$done->result;
#      my $host=$SOAPdata->{HOST};
#      my $port=$SOAPdata->{PORT};
#      $self->{SOAP}->{"$host$port"}= SOAP::Lite->uri("AliEn/Service/SE")
#	->proxy("http://$host:$port");
#      $self->info("Updating the cache");
#      $done=$self->{SOAP}->CallSOAP("$host$port", "updateLocalCache",
#				    $id, $data->{pfn});
#      $self->{SOAP}->checkSOAPreturn($done)
#	or $self->info( "Error updating the entry");
      
#      $self->info( "Cache updated");
#    } else {
#      $self->info( "Error contacting the IS");
#    }
  } else {
    $self->{LOGGER}->warning ("TransferManager", "In updateCatalogue type $data->{type} unknown");
  }
  
  my ($oldUser)=$self->{CATALOGUE}->execute("whoami");
  $self->{CATALOGUE}->execute("user", "-", $data->{user});
  if (@command) {
    $self->debug(1, "In updateCatalogue doing @command");
    my $done=$self->{CATALOGUE}->execute(@command);
    $self->info( "Catalogue updated with $done");
    if (!$done and $command[0]=~ /addMirror/){
      $self->info("Let's retry...");
      $done=$self->{CATALOGUE}->execute("addMirror", $data->{lfn}, $data->{destination}); 
      $self->info("This time we got $done\n\n");
    }
  }
  $self->updateCollection($id, $data->{jdl}, $data->{lfn});

  $self->{CATALOGUE}->execute("user", "-", $oldUser);

  return 1;
}

 sub updateCollection{
  my $self=shift;
  my $id=shift;
  my $jdl=shift;
  my $lfn=shift;

  $jdl or return 1;
  my $ca=Classad::Classad->new($jdl);
  $ca or return;
  my ($ok, @collections)=$ca->evaluateAttributeVectorString("Collection");
  $ok or return 1;
  
  foreach my $collection (@collections){
    $self->info("We have to update the collection '$collection'");

    my ($c)=$self->{CATALOGUE}->execute("type", $collection);
    if (!$c){
      $self->{CATALOGUE}->execute("createCollection", $collection) or
	$self->info("Error updating the collection '$collection'") and next;
    }
    $self->{CATALOGUE}->execute("addFileToCollection", $lfn, $collection);
  }

  return 1;
}

sub getNewRequirements {
  my $this=shift;
  my $status=shift;
  my $id=shift;
  my $options=shift;
  
  my $message="";

  my ($jdl)=$self->{DB}->getJdl($id);

  ($jdl) or $message="transfer $id does not have a jdl";

  my $ca;
  if (! $message){
    $ca=Classad::Classad->new($jdl);
    $ca or $message="doing the classad of $jdl";
  }
  $message and $self->{LOGGER}->error("TransferManager", "In getNewRequirements $message")
    and return;

    my $req="";

  $self->debug(1, "In getNewRequirements getting the new requirements $status");
  if ($status eq "LOCAL COPY")   {
    my ( $ok, $name)=$ca->evaluateAttributeString("ToSE");
    $name or
      $self->{LOGGER}->error("TransferManager", "In getNewRequirements ToSE is not defined")
	and return;
    $req="(other.Type==\"FTD\") && (member (other.CloseSE, \"$name\"))";
  }elsif ($status eq "CLEANING") {
    my ( $ok, $name)=$ca->evaluateAttributeString("FromFTD");
    $name or
      $self->{LOGGER}->error("TransferManager", "In getNewRequirements FromFTD is not defined")
	and return;
    $req="(other.Type==\"FTD\") && (other.Name==\"$name\")";
  }

  if ($req) {
    $self->debug(1, "New req= $req!!");
    $ca->set_expression("requirements", $req )
      or $self->{LOGGER}->error("TransferManager", "In getNewRequirements error putting requirements as $req ")
	and return;
  }
  
  foreach my $key (keys %{$options}) {
    $self->debug(1, "Setting $key");
    my $value=$options->{$key};
    if ( UNIVERSAL::isa( $value, "ARRAY" )) {
      map {$_="\"$_\""} @$value;
      $value= "{". join (",", @$value) ."}";
    } else {
      $value="\"$value\"";
    }
    $ca->set_expression($key, $value )
      or $self->{LOGGER}->error("TransferManager", "In getNewRequirements error putting $key as $value")
	and return (-1, "putting $key as $value");
  }

  return (1, $ca->asJDL());
}

sub checkTransfer {
    my $this=shift;
    my $id=(shift or "");
    $id or $self->{LOGGER}->error("TransferManager", "In checkTransfer transfer id is missing")
	and return (-1, "Error: no transfer id");
    $self->info( "Checking the status of $id");

    my ($data)=$self->{DB}->getFields($id, "status, jdl");

	defined $data
		or $self->{LOGGER}->error("JobManager","In checkTransfer error during execution of database query")
		and return (-1, "error during execution of database query");

	$data->{status} and
		$self->info( "Got $data->{status}")
		or  $self->{LOGGER}->error("TransferManager", "In checkTransfer transfer $id does not exist")
		and return (-1, "transfer $id does not exist");

    my @extra=();

    if ($data->{status} eq "DONE") {
      $data->{jdl}=~ s/^.*finalpfn\s*=\s*\"(\S+)\";.*$/$1/is;
      push @extra, $data->{jdl};
      $self->info( "Transfer is done. Returning the pfn '$data->{jdl}'");
    }

	return $data->{status}, @extra;
}


sub resubmitTransferHelp{
	my $self=shift;
	return "resubmitTransfer: resubmits a trasfer. If used with -reset the number of attempts is set to 0\tUsage:
\t\resubmitTransfer [-reset]";
}

sub resubmitTransfer {
	my $this=shift;
	my $id=(shift or "");	
	my $reset=(shift or 0);
	my $info;
	my $res;
	
	$id or $self->{LOGGER}->error("TransferManager", "In resubmitTransfer transfer id is missing")	and return (-1, "Transfer id is missing\n");
	
	$self->info( "Resubmitting transfer $id");
	
	$res = $self->{DB}->queryRow("SELECT * from TRANSFERS where transferId='$id' ") or die("Error getting the info of transfer $id\n");
	
	$res->{transferId} and die("Transfer $id doesn't exist\n");
		
	if($reset eq "-reset"){
		$info->{attempts}=0;
		$info->{status}="INSERTING";
		$self->{DB}->updateTransfer($id, $info) or $self->info( "Error resetting attemptes for transfer $id\n");
	}
		
	$res = $self->{DB}->updateStatus($id,"%", "INSERTING" ) or $self->{LOGGER}->error( "Error resubmitting transfer $id")
		and return (-1, "Error Updating Status");
	
	$self->{DB}->updateActions({todo=>1}, "action='INSERTING'")  or $self->{LOGGER}->error( "Error setting todo=1 for transfer $id")
		and return (-1, "Error setting todo=1");
	
	return 1;
}

 sub listTransfer_HELP{
  my $self=shift;

  return "listTransfer: returns all the transfers that are waiting in the system
\tUsage:
\t\tlistTransfer [-status <status>] [-user <user>] [-id <queueId>] [-verbose] [-master] [-summary] [-all_status] [-jdl] [-destination <site>]
";
}

sub listTransfer {
	my $this=shift;
	$self->info("Checking the list of transfers @_"); 
	my $args =join (" ", @_);
	my $date = time;
	
	$self->info( "Asking for the transfers..." );
	
	if ($args =~ /-?-h(elp)/) {
		$self->info("Returning the help message of top");
		return $self->listTransfer_HELP();
	}
	
	my $where=" WHERE 1";
	my $columns="transferId, status, destination, user, size,started, received, finished, attempts ";
	my $all_status=0;
	my $master=0;
	my $jdl=0;
	my $error="";
	my $data;
	
	
# 	ARRAY OF HASHES
	my @columns=(
		{name=>"user", pattern=>"u(ser)?",column=>"user"},
		{name=>"id", pattern=>"i(d)?",column=>"transferid"},
		{name=>"status", pattern=>"s(tatus)?",column=>"status"},
		{name=>"destination", pattern=>"d(estination)?",column=>"destination"},
# 		{name=>"persevere",pattern=>"p(ersevere)?",column=>"persevere"}
		{name=>"attempts",pattern=>"a(ttempts)?",column=>"attempts"}
		);
		
	
	while (@_) {
		my $argv=shift;
		#if argv contains summary, next 
		($argv=~ /^-?-summary$/) and next;
		($argv=~ /^-?-verbose=?/) and $all_status=1 and  next;
		($argv=~ /^-?-all_status=?/) and $all_status=1 and  next;
		($argv=~ /^-?-master=?/) and $master=1 and  next;
		($argv=~ /^-?-jdl=?/) and $jdl=1 and  next;
		($argv=~ /^-?-attempts=?/) and $jdl=1 and  next;
		my $found;    

		foreach my $column (@columns){
			if ($argv=~ /^-?-$column->{pattern}$/ ){
				$found=$column;
				last;
			}
		}
		
		$found or  $error="argument '$argv' not understood" and last;
		my $type=$found->{name};
				
		my $value=shift or $error="--$type requires a value" and last;
		$data->{$type} or $data->{$type}={'query'=>[],bind=>[]} ;
		my $c=$found->{column} || $found->{name};
		push @{$data->{$type}->{query}}, "$c= ?";
		push @{$data->{$type}->{bind}}, $value;
	}
	
	if ($error) {
		my $message="Error in top: $error\n".$self->listTransfer_HELP();
		$self->{LOGGER}->error("JobManager", $message);
		return (-1, $message);
	}
	
	my @bind=();
	
	foreach my $column (@columns){
		$data->{$column->{name}} or next;
		
		if ($master and $column->{name} eq "id"){
			$self->info("We want to return all the transfers of parent id ");
			my @new=();
			my @newB=();
			foreach my $entry ( @{$data->{id}->{query}} ){
				push @new, $entry;
				$entry=~ s/transferid/transferGroup/;
				push @new, $entry;
				my $bind=pop(@{$data->{id}->{bind}});
				push @newB, $bind, $bind;
			}
			$data->{id}={query=>\@new, bind=>\@newB};
			$self->info("NOW WE HAVE");
			use Data::Dumper;
			print Dumper($data->{id});
		}
		
		$where .= " and (".join (" or ", @{$data->{$column->{name}}->{query}} ).")";
		push @bind, @{$data->{$column->{name}}->{bind}};
	}
	
	$all_status or $data->{status} or $data->{id} or $where.=" and ( status!='FAILED' and status !='DONE' and status !='KILLED')";
	
	$where.=" ORDER by transferId";
	$jdl and $columns.=", jdl ";
	$self->info( "In getTop, doing query $columns, $where (@bind)" );
		
	my $rresult = $self->{DB}->query("SELECT $columns from TRANSFERS  $where", undef, {bind_values=>\@bind}) 	or $self->{LOGGER}->error( "JobManager", "In getTop error getting data from database" )
	and return (-1, "error getting data from database");
		
	my @entries=@$rresult;
	$self->info( "ListTransfer done with $#entries +1");
		
	return $rresult;
}

sub killTransfer {
  my $this   = shift;
  my $user = shift;


  my @return;
  # check for subjob's ....
  foreach my $transferId (@_) {
    $self->info("Killing transfer $transferId");
    my $status="KILLED";
    eval {
      my $rresult = $self->{DB}->queryRow("SELECT * from TRANSFERS where transferId='$transferId' ") or die("Error getting the info of transfer $transferId");
      $rresult->{transferId} or die("Transfer $transferId doesn't exist\n");
      $user =~  /^($rresult->{user})|(admin(ssl)?)$/ 
	or die("User $user not allowed to kill the transfer of $rresult->{user}\n");
      $rresult->{status}=~ /KILLED/ and 
	die("The transfer is already killed\n");
      my $children=$self->{DB}->getActiveSubTransfers($transferId);
      if ($children and @$children){
	$self->info("Let's kill the subtransfers of this transfer");
	$self->killTransfer($user, @$children);
      }
      $self->changeStatusTransfer($transferId, 'KILLED') or 
	die ("Error updating the database");
    };
    if ($@){
      $self->info("The transfer wasn't killed: $@");
      $status="FAILED ($@";
      chomp $status;
      $status.=")";
    }
    push @return, "$transferId -> $status";

  }
  return \@return;
}

sub findAlternativeSource {
  my $this=shift;
  my $id=shift;
  my $failedSE=shift;
  $self->info("We are trying to find an alternative source for transfer $id");

  eval {
    my $jdl=$self->{DB}->getJdl($id) or die("Error getting the jdl\n");
    use Data::Dumper;
    print Dumper($jdl);

    my $ca=Classad::Classad->new($jdl) or die("Error creating the classad from '$jdl'\n");
    my ($ok, @ses)=$ca->evaluateAttributeVectorString("OrigSE");
    $ok or die("Error getting the source SE");
    
    $self->info("Starting with @ses, and can't get it from $failedSE");
    map {$_="\"$_\""} @ses;
    @ses=grep(! /^\"$failedSE\"$/i, @ses);
    @ses or die("There are no other se :( ");
    
    $self->info("We could still transfer the file from @ses");
    $ca->set_expression("OrigSE", "{". join(",", @ses). "}");
    ($ok, my @failedSE)=$ca->evaluateAttributeVectorString("FailedSE");
    push @failedSE, $failedSE;
    map {$_="\"$_\""} @failedSE;

    $ca->set_expression("FailedSE",  "{". join(",", @failedSE). "}");
    $ca->insertAttributeString("Action", "local copy");
    map {$_="member(other.CloseSE, $_)"} @ses;
    
    my $req="(other.type==\"FTD\") && (". join(" || ",  @ses) .")";    
    $ca->set_expression("requirements", $req);
    $self->info("Everything looks ok. Let's update the database");
    my $newJDL=$ca->asJDL();
    use Data::Dumper;
    print $newJDL;

    $self->{DB}->updateTransfer($id, {jdl=>$newJDL, status=>'WAITING'}) or die("Error updating the datbase\n");
      
  };
    
  if ($@){
    $self->info("Error finding an alternative: $@");
    die("There are no alternative sources: $@\n");
  }

  $self->info("There are other alternatives for that transfer :)");
  return 1;
}


return 1;


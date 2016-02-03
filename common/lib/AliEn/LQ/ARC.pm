package AliEn::LQ::ARC;

# if you got any questions or suggestions about the module 
# please contact me at: Pavlo Svirin <pavlo.svirin@cern.ch>

use AliEn::LQ;
use AliEn::Config;
@ISA = qw( AliEn::LQ);
use strict;

use AliEn::Database::CE;
use File::Basename;
use File::Copy;

use Data::Dumper;

my $nRunningJobs             = 0;
my $nQueuedJobs              = 0;
my $getAllBatchIds_timestamp = 0;
my $last_arcclean_delay = 1;
my $last_validation_delay = 1;

my @debug_args = ();

# constants
# a delay to run arcsync if in $INFO_SYSTEM_DELAY the JA has not reached the infosyste,
my $INFO_SYSTEM_DELAY = 15;

# run arcclean once in
my $ARCCLEAN_RUN_DELAY = 60*24;

# verify jobs file once in
my $ARC_VALIDATE_JOBS_FILE_DELAY = 60;  

# job ids which have not reached the info system
my %notFoundIds = ();


sub initialize {
	my $self = shift;
	$self->info( $ENV{ARC_QUEUE_TYPE} );

	push( @debug_args, '-d DEBUG' ) if ( $ENV{ARC_DEBUG} );
	$self->{LOCALJOBDB} = new AliEn::Database::CE or return;
	
	$self->{CONFIG}->{LCGVO} = ( defined $ENV{ALIEN_VOBOX_ORG} ? 
										$ENV{ALIEN_VOBOX_ORG} : 
										$self->{CONFIG}->{ORG_NAME} );	
	
	$self->{CONFIG}->{CE_SITE_BDII} = $ENV{CE_SITE_BDII};
	$self->{CONFIG}->{CE_USE_BDII} = $ENV{CE_USE_BDII} || 0;
	$self->{CONFIG}->{CE_SUBMITARG} = $ENV{CE_SUBMITARG};
	$self->{CONFIG}->{CE_SUBMITARG_LIST} = $ENV{CE_SUBMITARG_LIST};
	$ARCCLEAN_RUN_DELAY = $ENV{ARCCLEAN_RUN_DELAY} if $ENV{ARCCLEAN_RUN_DELAY};
	$ARC_VALIDATE_JOBS_FILE_DELAY = $ENV{ARC_VALIDATE_JOBS_FILE_DELAY} if $ENV{ARC_VALIDATE_JOBS_FILE_DELAY}; 
			
	$self->readCEList;
						
	return 1;
}

#
#---------------------------------------------------------------------
#

# prepares and submits the JA ARC script
sub submit {
	my $self       = shift;
	my $classad    = shift;
	my $executable = shift;
	my $arguments  = join " ", @_;

	my $file   = "dg-submit.$$";
	my $tmpdir = "$self->{CONFIG}->{TMP_DIR}";
	if ( !( -d $self->{CONFIG}->{TMP_DIR} ) ) {
		my $dir = "";
		foreach ( split( "/", $self->{CONFIG}->{TMP_DIR} ) ) {
			$dir .= "/$_";
			mkdir $dir, 0777;
		}
	}
   
   	# getting hostnames for CE & submit line
   	my $submit_line = '';
   	if( $self->{CONFIG}->{CE_USE_BDII} ){
	   	map { $submit_line .= " -c " . (split ':')[0]; } keys %{$self->{CE_CLUSTERSTATUS}[0]};
	   	$submit_line .= " $ENV{CE_SUBMITARG}" if defined $ENV{CE_SUBMITARG};
	   	$self->debug( 1, "Submit string is: $submit_line" );
   	}

	my @args = ();
	$self->{CONFIG}->{CE_SUBMITARG}
	  # and @args = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
	  and @args = split / /, $self->{CONFIG}->{CE_SUBMITARG_LIST};

	my @xrsl = grep ( /^xrsl:/, @args );
	map { s/^xrsl:// } @xrsl;
	@args = grep ( !/^xrsl:/, @args );

	my $xrslfile = $self->generateXRSL( $classad, @xrsl );
	$xrslfile or return;

	my @command = ( "arcsub -f $xrslfile", 
						( $self->{CONFIG}->{CE_USE_BDII} ? $submit_line : @args ), 
						@debug_args );
	$self->info("Submitting to ARC with: @command\n");

	open SAVEOUT, ">&STDOUT";
	open SAVEOUT, ">&STDOUT";
	if ( !open STDOUT, ">$self->{CONFIG}->{TMP_DIR}/stdout" ) {
		return 1;
	}
	my $error = system("@command");
	close STDOUT;
	open STDOUT, ">&SAVEOUT";

	open( FILE, "<$self->{CONFIG}->{TMP_DIR}/stdout" ) or return 1;
	my $contact = <FILE>;
	$self->{LOGGER}->warning( "ARC", "Jobagent URI submitted is: $contact\n" );
	close FILE;
	$contact
	  and chomp $contact;
	if ( $contact =~ /gsiftp:\/\// ) {
		my @cstring = split( ":", $contact );
		$contact = "$cstring[1]:$cstring[2]:$cstring[3]";
	}
	else {
		$error = 1;
	}

	if ($error) {
		$contact or $contact = "";
		$self->{LOGGER}
		  ->warning( "ARC", "Error submitting the job. Log file '$contact'\n" );
		$contact
		  and $contact !~ /^:*$/
		  and system( 'cat', $contact, '>/dev/null 1>&2' );
		return $error;
	}
	else {
		$self->info("ARC JobID is $contact");
		$self->{LAST_JOB_ID} = $contact;
		open JOBIDS, ">>$self->{CONFIG}->{LOG_DIR}/CE.db/JOBIDS";
		print JOBIDS "$contact\n";
		close JOBIDS;
	}

	return $error;
}

#
#---------------------------------------------------------------------
#

sub kill {
	my $self    = shift;
	my $queueid = shift;
	$queueid or return;
	my ($contact) = $self->getContactByQueueID($queueid);
	if ( !$contact ) {
		$self->{LOGGER}->error( "LCG", "The job $queueid is not here" );
		return 1;
	}

	$self->info("Killing job $queueid, JobID is $contact");

	my $error = system( "arckill", "$contact" );
	return $error;
}

#
#---------------------------------------------------------------------
#

sub getBatchId {
	my $self = shift;
	return $self->{LAST_JOB_ID};
}

#
#---------------------------------------------------------------------
#

sub getStatus {
	my $self    = shift;
	my $queueid = shift;
	$queueid or return;
	$self->info("GetStatus: getting status from ARC for $queueid");
	my $ARCStatus = $self->getJobStatus($queueid);
	$ARCStatus or return 'DEQUEUED';
	chomp $ARCStatus;

	$self->debug( 1, "ARC Job $queueid is $ARCStatus" );
	if (   $ARCStatus eq "CANCELING"
		|| $ARCStatus eq "FINISHING"
		|| $ARCStatus eq "DELETED" )
	{
		return 'DEQUEUED';
	}
	if ( $ARCStatus =~ /^((FINISHED)|(FAILED))$/ ) {
		my ($contact) = $self->getContactByQueueID($queueid);
		$contact or return;
		return 'DEQUEUED';
	}
	return 'QUEUED';
}

#
#---------------------------------------------------------------------
#

sub getOutputFile {
	my $self    = shift;
	my $queueId = shift;
	my $output  = shift;

	$self->debug( 2, "QUEUEID: $queueId" );

	my $batchId = $self->getContactByQueueID($queueId);
	$self->debug( 2, "The job Agent is: $batchId" );

	system(
		"arccp $batchId/alien-job-$queueId/$output /tmp/alien-output-$queueId");

	open OUTPUT, "/tmp/alien-output-$queueId";
	my @data = <OUTPUT>;
	close OUTPUT;

	system->("rm /tmp/alien-output-$queueId");

	return join( "", @data );
}

#
#---------------------------------------------------------------------
#

sub getAllBatchIds {
	my $self = shift;
	
	$last_arcclean_delay %= $ARCCLEAN_RUN_DELAY;
	$last_validation_delay %= $ARC_VALIDATE_JOBS_FILE_DELAY;
	$self->info( "Last arcclean run $last_arcclean_delay mins ago" );
	$self->info( "Last jobs file verification run $last_validation_delay mins ago" );
	$self->cleanAllFinished and $self->repairJobsFile if( !$last_arcclean_delay );
	$self->repairJobsFile if $last_arcclean_delay 
							and !$last_validation_delay 
							and $self->validateJobsFile;
	$last_arcclean_delay++;
	$last_validation_delay++;
	
	### DEBUG
	# my @args = ();
	# $self->{CONFIG}->{CE_SUBMITARG}
	#  and @args = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
	# $self->info( 'SUBMITARG: ' . $self->{CONFIG}->{CE_SUBMITARG} );
	# $self->info( 'SUBMITARGLIST: ' . Dumper( \@{$self->{CONFIG}->{CE_SUBMITARG_LIST}} ) );
	# $self->info( 'args: ' . Dumper( \@args ) );
		
	if( $self->{CONFIG}->{CE_USE_BDII} ){
		$self->info('Running BDII-based getAllBatchIds routine');
		return $self->getCeBDIIStatus;
	}
	else{
		$self->info('Running arcstat-based getAllBatchIds routine');
		return $self->getCeArcstatStatus;
	}
}

#
#---------------------------------------------------------------------
#

sub cleanAllFinished {
	my $self = shift;
	$self->info( 'Running arcclean routinr for all finished JAs' );
	system('arcclean -a -s FINISHED');
	
	return 1;
}

#
#---------------------------------------------------------------------
#

sub getCeArcstatStatus {
	my $self        = shift;
	my $needArcSync = 0;

# Running arcstat can sometimes be slow, and the situation rarely changes much in a few seconds
#    if (time() <= $getAllBatchIds_timestamp + 5) {
#        $self->info("Reusing old arcstat result; found $nRunningJobs jobs, of which $nQueuedJobs queueing");
#        $self->info("Last stats collected: $getAllBatchIds_timestamp , time now is: " . time() );
#	$self->{LOGGER}->warning( "Last stats collected: $getAllBatchIds_timestamp , time now is: " . time() );
#        return ($nRunningJobs, $nQueuedJobs);
#    }

	$nRunningJobs = 0;
	$nQueuedJobs  = 0;

	open LB, "arcstat -a 2>&1 |" or next;
	my @output = <LB>;
	close LB;

	my ( $id, $status, @completedJobs );

	foreach my $entry (@output) {
		$entry =~ /^Job: (gsiftp.*)$/ and $id = $1 and next;
		if ( $entry =~ /^  *State:\s*\S+\s*\((\S+)\)/ ) {
			$status = $1;
			$id
			  or $self->info("Error: found a status, but there is no id")
			  and next;
			$self->debug( 1, "Id $id has status $status" );

# Remove completed jobs, unless we are in debug mode
#if ($status =~ /^((FINISHED)|(FAILED)|(KILLED))$/ and not $self->{LOGGER}->getDebugLevel()){
			if ( $status =~ /^((FINISHED)|(FAILED)|(KILLED)|(DELETED))$/
				and not $self->{LOGGER}->getDebugLevel() )
			{
				push @completedJobs, $id;

				# Removing many jobs at a time is faster than one at a time,
				# but too many may result in "too long argument list" errors.
				if ( @completedJobs >= 500 ) {
					$self->info( $#completedJobs + 1
						  . " completed jobs will be removed" );
					system( "arcclean " . join( " ", @completedJobs ) );
					@completedJobs = ();
				}
			}

			if ( $status !~
/^(CANCELING)|(FINISHED)|(FINISHING)|(DELETED)|(FAILED)|(KILLED)$/
			  )
			{
				$self->debug( 1, "The job is queued ($status)" );

				# $nRunningJobs += 1;
				if ( $status =~
/^(ACCEPTING)|(ACCEPTED)|(PREPARING)|(PREPARED)|(SUBMITTING)|(INLRMS:Q)$/
				  )
				{
					$nQueuedJobs += 1;
				}
				else {
					$nRunningJobs += 1;
				}
			}
			delete $notFoundIds{$id} if exists( $notFoundIds{$id} );
			undef $id;
		}
		elsif ( $entry =~
			/Job information not found in the information system: (gsiftp.*)/ )
		{
			$id = $1;

	 #$nRunningJobs += 1; # Probably just the info.sys being slow, or something.
			$nQueuedJobs += 1;

			# no!

	  # if not exists in the hash : add id to hash with value 1, nRunningJobs ++
			if ( exists( $notFoundIds{$id} ) ) {
				$notFoundIds{$id} += 1;
				$needArcSync = 1 if $notFoundIds{$id} >= $INFO_SYSTEM_DELAY;
			}
			else {
				$notFoundIds{$id} = 1;
			}

			$self->info("Info for job $id not found");
		}
		elsif ( $entry =~ /This job was very recently submitted/ ) {
			$nQueuedJobs += 1;    # Probably queuing
			$self->info("Job $id is probably queuing");
		}
	}

	if ( @completedJobs and not $self->{LOGGER}->getDebugLevel() ) {
		$self->info( $#completedJobs + 1 . " completed jobs will be removed" );
		system( "arcclean " . join( " ", @completedJobs ) );
	}

	if ($needArcSync) {
		$self->info( "Running arcsync as it seems that there are long time lost jobs on the cluster" );
		
		my @hosts_to_sync = ();
		map 
			{ push @hosts_to_sync, (split ( s/^gsiftp:\/\/// ),':') 
							if( $notFoundIds{$_} >= $INFO_SYSTEM_DELAY); } 
			keys %notFoundIds;
		@hosts_to_sync = _uniq( @hosts_to_sync );
		foreach( @hosts_to_sync ){
			system( "arcsync -T -f -c " . $_ );
			$self->info("arcsync finished with code $?") if ( !$? );
		}
		
		delete @notFoundIds{ keys %notFoundIds };
		$needArcSync = 0;		
	}

	$self->debug( 1, 'Not found URIs: ' . Dumper( \%notFoundIds ) );

	$getAllBatchIds_timestamp = time();
	$self->info( "Found "
		  . ( $nRunningJobs + $nQueuedJobs )
		  . " jobs, of which $nQueuedJobs queueing" );
	return ( $nRunningJobs, $nQueuedJobs );
}

#
#---------------------------------------------------------------------
#

sub _uniq {
    my %seen;
    grep !$seen{$_}++, @_;
}

#
#---------------------------------------------------------------------
#

sub getNumberRunning() {
	my $self = shift;
	my @r    = $self->getAllBatchIds();
	return $r[0];
}

#
#---------------------------------------------------------------------
#

sub getNumberQueued() {
	my $self = shift;
	my @r    = $self->getAllBatchIds();
	return $r[1];
}

#
#---------------------------------------------------------------------
#

sub getContactByQueueID {
	my $self    = shift;
	my $queueId = shift;

	my $info =
	  $self->{LOCALJOBDB}->query( "SELECT batchId FROM JOBAGENT where jobId=?",
		undef, { bind_values => [$queueId] } );

	my $batchId = (@$info)[0]->{batchId};

	$self->info("The job $queueId run in the ARC job $batchId");

	return $batchId;
}

#
#---------------------------------------------------------------------
#

sub validateJobsFile{
	my $self = shift;
	$self->info( 'Starting job file validation' );
	my $fileSpec = $ENV{"HOME"} . "/.arc/jobs.dat";
	if ( ! -e $fileSpec ) {
		$fileSpec = $ENV{"HOME"} . "/.arc/jobs.xml";
		return -256 if ! -e $fileSpec;
	}	

	my $validationTool = 'db_verify';
	my $validationCmdLine = '';
	# which d_verify / xmllint
	if( $fileSpec =~ /xml$/ ){
		$validationTool = 'xmllint';
		$validationCmdLine = '--noout';
	}
	

	# run verification procedure 
	$validationCmdLine = "$validationTool $fileSpec $validationCmdLine";
	$self->info( "Running validation with: $validationCmdLine" );
	system( $validationCmdLine );

	$self->info( "Validation command returned: $?" );

	# return result
	return $?;
}

#
##---------------------------------------------------------------------
#

sub repairJobsFile{
	my $self = shift;
	$self->info( 'Repairing jobs file' );
        my $ce_list = $self->getCEString;
	# run system command
	my $start_time = time();
	system( "arcsync -T -f $ce_list" );
	$self->info('Repair process took: ' . time() - $start_time);
	$self->info( "Repair process returned: $?" );	

	return 1;
}

#
###---------------------------------------------------------------------
#

sub getCEString{
	my $self = shift;
    # get ce -c joined list
	my $ce_line = '';
    if( $self->{CONFIG}->{CE_USE_BDII} ){
		map { $ce_line .= " -c " . (split ':')[0]; } keys %{$self->{CE_CLUSTERSTATUS}[0]};	
	}
	elsif( $ENV{CE_SUBMITARG} ){
		my @ces = $ENV{CE_SUBMITARG} =~ /(-c [^\s]+)/g;
		$ce_line = "@ces";
	}	
	return $ce_line;
}

#
##---------------------------------------------------------------------
#

sub generateXRSL {
	my $self = shift;
	my $ca   = shift;
	my @args = @_;

	#my $requirements = $self->translateRequirements($ca);
	#   my $file = "dg-submit.$$";
	#   my $tmpdir = "$self->{CONFIG}->{TMP_DIR}";
	#   if ( !( -d $self->{CONFIG}->{TMP_DIR} ) ) {
	#     my $dir = "";
	#     foreach ( split ( "/", $self->{CONFIG}->{TMP_DIR} ) ) {
	#       $dir .= "/$_";
	#       mkdir $dir, 0777;
	#     }
	#   }

	map { /^\s*\(.*\)\s*$/ or s/^(.*)$/\($1\)/ } @args;
	@args or @args = ("");
	my $args = join( "\n", @args );
	$args or $args = "";

	$self->info("Hello  world with $args");
	my ( $ok, $ttl ) = $ca->evaluateExpression("Requirements");
	$self->info("Requirements $ttl");
	if ( $ttl and $ttl =~ /TTL\s*[=>]*\s*(\d+)/ ) {
		$self->info("Translating \'TTL\' requirement ($1)");
		my $minutes = int( $1 / 60 );
		$ttl = "(cpuTime=\"$minutes minutes\")\n";

		#$ttl = "(cpuTime=\"2880 minutes\")\n";
	}
	else {
		$ttl = "";
	}
	my $fullName = AliEn::TMPFile->new( { filename => "arc-submit.$$" } )
	  or return;
	my $jobscript = "$self->{CONFIG}->{TMP_DIR}/agent.startup.$$";
	$self->info("using jobscript $jobscript");
	my $file = $fullName;
	$file =~ s/^.*\/([^\/]*)$/$1/;
	$self->info("File name $file and fullName $fullName and");

	# proxyname to be submitted with
	my $proxyName = $ENV{SUBMITTED_PROXY_NAME} || "proxy";

	open( BATCH, ">$fullName.xrsl" )
	  or print STDERR "Can't open file '$fullName.xrsl': $!" and return;
	print BATCH "&
(jobName = \"AliEn-$file\")  
(executable = /usr/bin/time)
(arguments = bash \"$file.sh\")
(stdout = \"std.out\")
$args
$ttl
(stderr = \"std.err\")
(gmlog = gmlog)
(inputFiles = (\"$file.sh\" \"$jobscript\") ( \"$proxyName\" \"$ENV{X509_USER_PROXY}\" ))
(outputFiles = ( \"std.err\" \"\") (  \"std.out\"  \"\") (  \"gmlog\"  \"\")(\"$file.sh\" \"\") )
(*environment = (ALIEN_JOBAGENT_ID $ENV{ALIEN_JOBAGENT_ID})(ALIEN_CM_AS_LDAP_PROXY $ENV{ALIEN_CM_AS_LDAP_PROXY})(ALIEN_SE_MSS $ENV{ALIEN_SE_MSS})(ALIEN_SE_FULLNAME $ENV{ALIEN_SE_FULLNAME})(ALIEN_SE_SAVEDIR $ENV{ALIEN_SE_SAVEDIR})*)
(*environment = (ALIEN_JOBAGENT_ID $ENV{ALIEN_JOBAGENT_ID})(ALIEN_CM_AS_LDAP_PROXY $ENV{ALIEN_CM_AS_LDAP_PROXY})(ALIEN_SE_MSS file)(ALIEN_SE_FULLNAME ALIENARCTEST::ARCTEST::file)(ALIEN_SE_SAVEDIR /disk/global/aliprod/AliEn-ARC_SE,5000000)*)
(environment = (ALIEN_JOBAGENT_ID $ENV{ALIEN_JOBAGENT_ID})(ALIEN_CM_AS_LDAP_PROXY $ENV{ALIEN_CM_AS_LDAP_PROXY}) )
";
	close BATCH;

	return "$fullName.xrsl";
}

sub getScriptSpecifics {
	my $self            = shift;
	my $original_string = shift;
	return "export X509_USER_PROXY=\`pwd\`/proxy\n" . $original_string;
}

#
#---------------------------------------------------------------------
#

sub readCEList {
   my $self = shift;
   unless ($ENV{CE_LCGCE}) {
     $self->{LOGGER}->error("LCG", "No CE list defined in \$ENV");
     return;
   }
   my $string = $ENV{CE_LCGCE};
   my $clusters = [];
   my @sublists = ($string =~ /\(.+?\)/g);
   $string =~ s/\($_\)\,?// foreach (@sublists);
   push  @sublists, split(/,/, $string);
   foreach (@sublists) {
     s/\s*//g;
     s/\(//g;
     s/\)//g;
     my @list = split /,/;
     my $hash = {};
     $hash->{$_} = 0 foreach @list;
     push @$clusters,$hash;
   }
   $self->{CE_CLUSTERSTATUS} = $clusters;
   $self->info("Clusters configuration:\n".Dumper $self->{CE_CLUSTERSTATUS});
   return 1;
}

#
#---------------------------------------------------------------------
#

sub getCeBDIIStatus{
	my $self = shift;
				
	my $base = '';
	my $host = '';
	if( $self->{CONFIG}->{CE_USE_BDII} ){
		if ( defined $self->{CONFIG}->{CE_SITE_BDII} ){
			my $BDII_host = $self->{CONFIG}->{CE_SITE_BDII} =~ s/ldap:\/\///; 					
			($host, $base ) = ( $self->{CONFIG}->{CE_SITE_BDII} =~ /([^\/]+)\/(.*)/ );
		}
		else{
			$self->{LOGGER}->warning( "ARC", 'Error: USE_BDII specified but no CE_SITE_BDII' );
			return ( undef, undef );
		}
	}
		
	# make filter
	my $filter = "GlueVoViewLocalId=$self->{CONFIG}->{LCGVO}"; 
			
	# run query
	my $res = $self->queryBDII(
				$host,
				$filter,
				$base,
				qw(GlueCEStateRunningJobs GlueCEStateWaitingJobs GlueChunkKey )
	);
		
	my $running = 0;
	my $waiting = 0;
	my %ce_data = %{$res};
	$self->debug( 1, Dumper( $res ) );
		
	foreach my $ce ( keys %{$self->{CE_CLUSTERSTATUS}[0]} ){				
		$running += $res->{$ce}->{GlueCEStateRunningJobs};
		$waiting += $res->{$ce}->{GlueCEStateWaitingJobs};
	}
	
	$self->info( "Returning: ( $running, $waiting )" );		
	return ( $running, $waiting );
}

#
#---------------------------------------------------------------------
#

sub queryBDII {
	my $self   = shift;
	my $CE     = shift;    #GlueCEUniqueID
	my $filter = shift;
	$filter or $filter = "objectclass=*";
	my $base = shift;
	my @items   = @_;
	my %results = ();
	
	my %ce_data = ();
	
	$self->info("Querying $CE for @items");
	$self->debug( 1, "DN string is $base" );
	$self->debug( 1, "Filter is $filter" );

	( my $host, undef ) = split( /:/, $CE );
	my $IS = "ldap://$host:2170/mds-vo-name=resource,o=grid";    # Resource BDII
	$IS = $self->{CONFIG}->{CE_SITE_BDII}
	  if ( defined $self->{CONFIG}->{CE_SITE_BDII} );
	$IS =~ s/^ldap:\/\///;
	$self->info( "IS host: " . $IS );	
	my $ldap = '';
	my ( $GRIS, $BaseDN ) = split( /\//, $IS, 2 );
	$self->info( "Asking $GRIS/$BaseDN" );

	unless ( $ldap = Net::LDAP->new($GRIS) ) {
		$self->info("$GRIS/$BaseDN not responding (1)");
		return;
	}
	unless ( $ldap->bind() ) {
		$self->{LOGGER}->info("$GRIS/$BaseDN not responding (2)");
		next;
	}
	my $result = $ldap->search(
		base => "$base",
		filter => "$filter"
	);
	
	my $code = $result->code;
	my $msg  = $result->error;
	if ($code) {
		$self->{LOGGER}->warning( "LCG", "\"$msg\" ($code) from $GRIS/$BaseDN" );
		return;
	}
	
	foreach my $entry ($result->entries) {
		my $ce_name = $entry->get_value('GlueChunkKey');
		$ce_name =~ s/GlueCEUniqueID=//; 

		foreach my $attr ( @items ){
			$ce_data{$ce_name}{$attr} = $entry->get_value( $attr );			
		}
	}
	
	return \%ce_data;
}

return 1;

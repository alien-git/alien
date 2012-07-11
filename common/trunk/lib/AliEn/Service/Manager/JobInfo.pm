package AliEn::Service::Manager::JobInfo;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use strict;

use AliEn::Database::TaskQueue;
use AliEn::Service::Manager;
use AliEn::JOBLOG;
use AliEn::Util;
use Classad;

use AliEn::Database::IS;

use vars qw (@ISA $DEBUG);
@ISA=("AliEn::Service::Manager");

$DEBUG=0;

my $self = {};

sub initialize {
  $self     = shift;
  my $options =(shift or {});
  $DEBUG and $self->debug(1, "In initialize initializing service JobManager" );
  $self->{SERVICE}="JobInfo";
  $self->{DB_MODULE}="AliEn::Database::TaskQueue";
  $self->SUPER::initialize($options);

  $self->{JOBLOG} = new AliEn::JOBLOG();

  $self->{DB_I}=AliEn::Database::IS->new({ROLE=>'admin'}) or return;
  
  return $self;
}

##############################################################################
# Public functions
##############################################################################
sub alive {
  my $this = shift;
  my $host    = shift;
  my $port    = shift;
  my $site    = shift;
  my $version = ( shift or "" );
  my $free_slots= (shift or "");

  my $date = time;

  $self->info("Host $host (version $version) is alive" );

  my ($error) = $self->{DB}->getFieldFromHosts($host,"hostId");

  if ( !$error ) {
    $self->InsertHost($host, $port)
      or return (-1, $self->{LOGGER}->error_msg);
  }

  $self->info("Updating host $host" );

  if (!$self->{DB}->updateHost($host,{status=>'CONNECTED', connected=>1, hostPort=>$port, date=>$date, version=>$version})) {
    $DEBUG and $self->debug(1, "In alive unable to update host $host" );
    return;
  }
  if ($site ne "") {
    my $blocking = $self->_getSiteQueueBlocked($site);
	
    if ($blocking ne "open" ) {
      $self->info("The site $site is blocked in the master queue!");
      $self->setSiteQueueStatus($site, "closed-blocked");
      return "-2";
    }
  }

  $DEBUG and $self->debug(1, "In alive finished updating host $host" );

  my %queue=$self->GetNumberJobs($host,$site, $free_slots);


  $self->info( "Maximum number of jobs $queue{maxjobs} ($queue{maxqueuedjobs} queued)" );

  $self->setAlive();
  return {%queue};
}

sub GetNumberJobs {
  my $this=shift;
  my $host=shift;
  my $site=shift;
  my $free_slots=shift;

  $DEBUG and $self->debug(1,"In GetNumberJobs fetching maxJobs, maxQueued, queues for host $host");

  my $data = $self->{DB}->getFieldsFromHosts($host,"maxJobs, maxQueued, queues")
    or $self->info("There is no data for host $host")
		and return;

  $DEBUG and $self->debug(1,"In GetNumberJobs got $data->{maxJobs},$data->{maxQueued},$data->{queues}");

  my %queue = split ( /[=;]/, $data->{queues} );
  $queue{"maxjobs"} = $data->{maxJobs};
  $queue{'maxqueuedjobs'}=$data->{maxQueued};

  if (($data->{maxJobs} eq "-1") && $free_slots){
    $queue{maxjobs}= $queue{maxqueuedjobs}=$free_slots;
  }

  if ($site ne "") {
    my $queuestat;
    $DEBUG and $self->debug(1,"Getting site statistics for $site ...");
    $queuestat = $self->_getSiteQueueStatistics($site);
    $DEBUG and $self->debug(1,"Got site statistics for $site...");
    # copy the additional information into the queue hash
    my $qhash;
    foreach $qhash (@$queuestat) {
      $DEBUG and $self->debug(1,"Processing Queue hash...");
      foreach ( keys %$qhash ) {
	$DEBUG and $self->debug(1,"Looping Queue hash $_ ... $qhash->{$_}");
	$queue{$_} = $qhash->{$_};
	$DEBUG and $self->debug(1,"Status $site: $_ = $queue{$_}");
      }
    }
  }

  return %queue;
}

sub InsertHost {
  my $this =shift;
  my $host =shift;
  my $port =shift;
  my $domain;

  $self->info("Inserting new host $host" );

  ( $host =~ /^[^\.]*\.(.*)$/ ) and $domain = $1;

  ($domain)
    or $self->{LOGGER}->error( "JobManager", "In InsertHost domain of $host not known" )
      and return;

  $self->info("Domain is '$domain'" );

  my $domainId = $self->{DB}->getSitesByDomain($domain,"siteId");

  defined $domainId
    or $self->{LOGGER}->warning( "JobManager", "In InsertHost error during execution of database query" );

  if (!(defined $domainId) || !(@$domainId)) {
    my $domainSt=$self->{CONFIG}->getInfoDomain($domain);
    $domainSt
      or $self->{LOGGER}->error( "JobManager", "In InsertHost domain $domain not known in the LDAP server" )
	and return;

    $self->info("Domain: $domainSt->{DOMAIN}; domain name: $domainSt->{OU}");

    $DEBUG and $self->debug(1, "In InsertHost inserting new site");
    $self->{DB}->insertSite(
			    {siteName=>$domainSt->{OU},
			     siteId=>'',
			     masterHostId=>'',
			     adminName=>$domainSt->{ADMINISTRATOR} || "",
			     location=>$domainSt->{LOCATION} || "",
			     domain=>$domain,
			     longitude=>$domainSt->{LONGITUDE} || "",
			     latitude=>$domainSt->{LATITUDE} || "",
			     record=>$domainSt->{RECORD} || "",
			     url=>$domainSt->{URL} || ""})
      or $self->{LOGGER}->error( "JobManager", "In InsertHost error inserting the domain $domainSt->{DOMAIN} in the database" )
	and return;

    $domainId =$self->{DB}->getSitesByDomain($domain,"siteId");

    defined $domainId
      or $self->{LOGGER}->warning( "JobManager", "In InsertHost error during execution of database query" )
	and return;

    @$domainId
      or $self->{LOGGER}->error( "JobManager", "In InsertHost insertion of the domain $domainSt->{DOMAIN} did not work" )
	and return;


  }
  $domainId = $domainId->[0]->{"siteId"};

  $DEBUG and $self->debug(1, "Inserting a new host");

  $self->{DB}->insertHostSiteId($host,$domainId)
    or $self->{LOGGER}->error( "JobManager", "In InsertHost insertion of the host $host did not work" )
      and return;

  $self->info("Host $host inserted");

  return 1;
}




sub getExecHost {
  my $this   = shift;
  my $queueId = shift;

  ($queueId)
    or $self->{LOGGER}->error( "JobManager", "In getExecHost queueId not specified" )
      and return(-1, "No queueid");

  $self->info( "Getting exechost of $queueId");

  my $date = time;

  $DEBUG and $self->debug(1, "In getExecHost asking for job $queueId" );

  my ($host) = $self->{DB}->getFieldFromQueue($queueId,"execHost");

  ($host) or $self->info("Error getting the host of $queueId" )
    and return (-1, "no host");

  $host =~ s/^.*\@//;

  my ($port) = $self->{DB}->getFieldFromHosts($host,"hostPort")
    or $self->info("Unable to fetch hostport for host $host" )
      and return (-1, "unable to fetch hostport for host $host");

  $self->info( "Done $host and $port");
  return "$host###$port";
}

sub getJobInfo {
  my $this = shift;
  my $username = shift;
  my @jobids=@_;
  my $date = time;
  my $result=
    my $jobtag;

  my $cnt=0;
  foreach (@jobids) {
    if ($cnt) {
      $jobtag .= " or (queueId = $_) or (split = $_) ";
    } else {
      $jobtag .= " (queueId = $_) or (split = $_) ";
    }
    $cnt++;
  }

  $self->info( "Asking for Jobinfo by $username and jobid's @jobids ..." );
  my $allparts = $self->{DB}->getFieldsFromQueueEx("count(*) as count, min(started) as started, max(finished) as finished, statusId", " WHERE $jobtag GROUP BY statusId");

  for (@$allparts) {
    $result->{$_->{statusId}} = $_->{count};
  }
  return $result;
}

sub getSystem {
  my $this = shift;
  my $username = shift;
  my @jobtag=@_;
  my $date = time;

  $self->info( "Asking for Systeminfo by $username and jobtags @jobtag..." );
  my $jdljobtag;
  my $joinjdljobtag;
  $joinjdljobtag = join '%',@jobtag;
  
  if ($#jobtag >= 0) {
    $jdljobtag = "JDL like '%Jobtag = %{%$joinjdljobtag%};%'";
  } else {
    $jdljobtag="JDL like '%'"; 
  }
  
  $self->info( "Query does $#jobtag $jdljobtag ..." );
  my $allparts = $self->{DB}->getFieldsFromQueueEx("count(*) as count, statusId", "WHERE $jdljobtag GROUP BY statusId");
  
  my $userparts = $self->{DB}->getFieldsFromQueueEx("count(*) as count, statusId", "WHERE submitHost like '$username\@%' and $jdljobtag GROUP BY statusId");
  
  my $allsites  = $self->{DB}->getFieldsFromQueueEx("count(*) as count, site"," WHERE $jdljobtag Group by site");

  my $sitejobs =$self->{DB}->getFieldsFromQueueEx("count(*) as count, site, statusId", "WHERE $jdljobtag GROUP BY concat(site, statusId)");

  my $totalcost = $self->{DB}->queryRow("SELECT sum(cost) as cost FROM QUEUE WHERE $jdljobtag"); 

  my $totalusercost = $self->{DB}->queryRow("SELECT sum(cost) as cost FROM QUEUE WHERE submitHost like '$username\@%' and $jdljobtag");
  
  my $totalUsage = $self->{DB}->queryRow("SELECT sum(cpu*cpuspeed/100.0) as cpu,sum(rsize) as rmem,sum(vsize) as vmem FROM QUEUE WHERE statusId=10 and $jdljobtag");
  
  my $totaluserUsage = $self->{DB}->queryRow("SELECT sum(cpu*cpuspeed/100.0) as cpu,sum(rsize) as rmem,sum(vsize) as vmem FROM QUEUE WHERE submitHost like '$username\@%' and statusId=10 and $jdljobtag");
  
  my $resultreturn={};
  
  $resultreturn->{'totcpu'} = ($totalUsage->{cpu} or 0);
  $resultreturn->{'totrmem'}= ($totalUsage->{rmem} or 0);
  $resultreturn->{'totvmem'}= ($totalUsage->{vmem} or 0);
  $resultreturn->{'totcost'}= ($totalcost->{cost} or 0);
  $resultreturn->{'totusercost'}= ($totalusercost->{cost} or 0);
  $resultreturn->{'totusercpu'}  = ($totaluserUsage->{cpu} or 0);
  $resultreturn->{'totuserrmem'} = ($totaluserUsage->{rmem} or 0);
  $resultreturn->{'totuservmem'} = ($totaluserUsage->{vmem} or 0);


  foreach my $status (@{AliEn::Util::JobStatus()}) {
    $resultreturn->{"nuser".lc($status)}=0;
    $resultreturn->{"n".lc($status)}=0;
  }


  for (@$allparts) {
    my $type=lc($_->{statusId});
    $resultreturn->{"n$type"}=$_->{count};
  }

  for my $info (@$userparts) {
    foreach my $status (@{AliEn::Util::JobStatus()}) {
      if ($info->{statusId} eq lc($status)) {
	$resultreturn->{"nuser$info->{status}"} = $info->{count};
	last;
      }
      
    }
  }
    
  my @sitestatistic = ( );
  my $arrayhash;
  # create the headers
  push @sitestatistic, [("Site","Done","Run","Save","Zomb","Queu","Start",
			 "Error","Idle","Iact")];


  foreach $arrayhash (@$allsites) {
    my @sitearray = ( );
    if ((!($arrayhash->{site})) or ($arrayhash->{site} eq '0')) {
      next;
    }
    $DEBUG and $self->debug(1, "Cheking site $arrayhash->{site}");
    push @sitearray, $arrayhash->{site};
    my $site={};
    foreach (@$sitejobs) {
      if ($arrayhash->{site} eq $_->{site}) {
	$site->{$site->{statusId}}=$_->{count};
      }
    }
    push @sitearray, ($site->{DONE} or "0");
    push @sitearray, ($site->{RUNNING} or "0");
    push @sitearray, ($site->{SAVING} or "0");
    push @sitearray, ($site->{ZOMBIE} or "0");
    push @sitearray, ($site->{QUEUED} or "0");
    push @sitearray, ($site->{STARTED} or "0");
    my $totalError=0;
    foreach (grep (/^ERROR_/, keys %{$site})){
      $totalError+=$site->{$_};
    }
    push @sitearray, $totalError;
    push @sitearray, ($site->{IDLE} or "0");
    push @sitearray, ($site->{INTERACTIVE} or "0");
    
    push @sitestatistic, [@sitearray];
  }
  
  foreach (@{AliEn::Util::JobStatus()}){
    my $var=lc($_);
    $resultreturn->{"frac$var"}=100.0;
    if ($resultreturn->{"n$var"}) {
      $resultreturn->{"frac$var"} = 100.0*$resultreturn->{"nuser$var"}/$resultreturn->{"n$var"};
    }
  }

  $resultreturn->{'efficiency'}     = 100.0;
  $resultreturn->{'userefficiency'} = 100.0;
  $resultreturn->{'assigninefficiency'} = 0.0;
  $resultreturn->{'userassigninefficiency'} = 0.0;
  $resultreturn->{'executioninefficiency'} = 0.0;
  $resultreturn->{'userexecutioninefficiency'} = 0.0;
  $resultreturn->{'submissioninefficiency'} = 0.0;
  $resultreturn->{'usersubmissioninefficiency'} = 0.0;
  $resultreturn->{'expiredinefficiency'}= 0.0;
  $resultreturn->{'userexpiredinefficiency'} =0.0;
  $resultreturn->{'validationinefficiency'}= 0.0;
  $resultreturn->{'uservalidationinefficiency'}= 0.0;    

  $resultreturn->{'nbaseefficiency'} = $resultreturn->{'ndone'} + $resultreturn->{'nerror_a'} + $resultreturn->{'nerror_e'} + $resultreturn->{'nerror_s'} + $resultreturn->{'nerror_r'} + $resultreturn->{'nexpired'} + $resultreturn->{'nzombie'};
  $resultreturn->{'nuserbaseefficiency'} = $resultreturn->{'nuserdone'} + $resultreturn->{'nusererror_a'} + $resultreturn->{'nusererror_e'} + $resultreturn->{'nusererror_s'} + $resultreturn->{'nusererror_r'} + $resultreturn->{'nuserexpired'} + $resultreturn->{'nzombie'};
    
  if  ($resultreturn->{'nbaseefficiency'}) {
    my $d=100.0 / $resultreturn->{'nbaseefficiency'};
    $resultreturn->{'efficiency'}        = $d * $resultreturn->{'ndone'};
    $resultreturn->{'assigninefficiency'}  = $d* $resultreturn->{'nerror_a'};
    $resultreturn->{'executioninefficiency'} = $d * $resultreturn->{'nerror_e'};
    $resultreturn->{'submissioninefficiency'} = $d * $resultreturn->{'nerror_s'};
    $resultreturn->{'expiredinefficiency'}    = $d * $resultreturn->{'nexpired'};
    $resultreturn->{'validationinefficiency'}    = $d * ($resultreturn->{'nerror_v'}+$resultreturn->{'nerror_vt'});
  }
  if ($resultreturn->{'nuserbaseefficiency'}) {
    my $d=100.0/$resultreturn->{'nuserbaseefficiency'};
    $resultreturn->{'userefficiency'} = $d * $resultreturn->{'nuserdone'};
    $resultreturn->{'userassigninefficiency'}  = $d * $resultreturn->{'nusererror_a'};
    $resultreturn->{'userexecutioninefficiency'} = $d * $resultreturn->{'nusererror_e'};
    $resultreturn->{'usersubmissioninefficiency'} = $d * $resultreturn->{'nusererror_s'};
    $resultreturn->{'userexpiredinefficiency'}    = $d * $resultreturn->{'nuserexpired'};
    $resultreturn->{'uservalidationinefficiency'} = $d * ($resultreturn->{'nusererror_v'}+$resultreturn->{'nusererror_vt'});
  }

  $resultreturn->{'sitestat'} = "";

  for my $i ( 0 .. $#sitestatistic ) {
    my $aref = $sitestatistic[$i];
    my $n = @$aref - 1;
    for my $j ( 0 .. $n ) {
      $resultreturn->{'sitestat'} .= $sitestatistic[$i][$j];
      $resultreturn->{'sitestat'} .= "#";
    }
    $resultreturn->{'sitestat'} .= "###";		    
  }

  return ($resultreturn);
}

sub getTrace {
  my $this = shift;
  if ($_[0] and ref $_[0] eq "ARRAY"){
    my $ref=shift;
    @_=@$ref;    
  }
  $self->info( "Asking for trace @_ $#_ ..." );
  my $jobid =shift;
  $jobid and   $jobid =~ /^trace$/ and $jobid=shift;
  $jobid  or return (-1,"You have to specify a job id!");

  $self->info( "... for job $jobid" );
  
  my @results={};
  if ($_[0] eq ""){
    @results=$self->{JOBLOG}->getlog($jobid,"state");
  } else {
    @results=$self->{JOBLOG}->getlog($jobid,@_);
  }
  return join ("",@results);
}

=item getJobRc

Gives the return code of a job

=cut


sub getSpyUrl {
    my $this    = shift;
    my $queueId = shift  or return;
    $self->info("Get Spy Url for $queueId"); 
    my ($url)  = $self->{DB}->getFieldFromQueue($queueId,"spyurl");
    $url or $self->info("In spy cannot get the spyurl for job $queueId");
    $self->info("Returning Spy Url for $queueId $url"); 
    return $url;
}


sub jobinfo {
  my $this = shift;
  my $site = shift or return;
  my $status = shift or return;
  my $delay = shift or return;
  my $now = time;
    
  my $array = $self->{DB}->getFieldsFromQueueEx("q.queueId","q, QUEUEPROC p where site like '$site' and statusId=$status and ( ($now - procinfotime) > $delay) and q.queueid=p.queueid");

  if (@$array) {
    return $array;
  } else {
    my @array;
    my $emptyjob={};
    $emptyjob->{queueId}=0;
    push @array,$emptyjob;
    return \@array;
  }
}

sub spy {
    my $this = shift;
    my $queueId = shift;
    my $file    = shift;

    my ($site) = $self->{DB}->getFieldsFromQueue($queueId,"site");

    $self->info("In spy contacting the IS at http://$self->{CONFIG}->{IS_HOST}:$self->{CONFIG}->{IS_PORT} for $queueId at $site->{'site'}...");


    my $result = $self->{DB_I}->getActiveServices("ClusterMonitor","host,port,protocols,certificate,uri",$self->{site});
   use Data::Dumper;
    $self->info(Dumper($result));


    #$self->info("In spy got http://$result->{'HOST'}:$result->{'PORT'}  ...");

    my $url=$self->getSpyUrl($queueId);
    $url or return (-1,"The job $queueId is no longer in the queue");
    $self->info("Telling the user to try with $url");
    return $url;

#    return $result2->result;
}

sub _getSiteQueueBlocked {
    my $self = shift;
    my $site = shift;
    my $blocking = $self->{DB}->getFieldsFromSiteQueueEx("blocked","where site='$site'");
    @$blocking and return @$blocking[0]->{'blocked'};
    return;
}
sub _getSiteQueueStatistics {
    my $self = shift;
    my $site = shift;
    return $self->{DB}->getFieldsFromSiteQueueEx( join(", ",@{AliEn::Util::JobStatus()}) ,"where site='$site'");
}

sub _setSiteQueueBlocked {
    my $self = shift;
    my $site = shift;
    my $set={};
    $set->{'blocked'} = 'locked-error-sub';
    if ($ENV{ALIEN_IGNORE_BLOCK}){
      $self->info("IGNORING THE BLOCKING");
      return 1;
    }
    return $self->{DB}->updateSiteQueue($set,"site='$site'");
}

sub setSiteQueueBlocked {
    my $this = shift;
    my $site = shift;
    return $self->_setSiteQueueBlocked($site);
}

sub getSiteQueueStatistics {
    my $this = shift;
    my $site = shift;
    return _getSiteQueueStatistics($site);
}

sub setSiteQueueStatus {
  my $this = shift;
  return $self->{DB}->setSiteQueueStatus(@_);
}


1;



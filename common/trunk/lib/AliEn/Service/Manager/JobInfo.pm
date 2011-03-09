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
use AliEn::Database::Admin;
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

sub getTop {
  my $this = shift;
  my $args =join (" ", @_);
  my $date = time;

  my $usage="\n\tUsage: top [-status <status>] [-user <user>] [-host <exechost>] [-command <commandName>] [-id <queueId>] [-split <origJobId>] [-all] [-all_status] [-site <siteName>]";

  $self->info( "Asking for top..." );

  if ($args =~ /-?-h(elp)/) {
    $self->info("Returning the help message of top");
    return ("Top: Gets the list of jobs from the queue$usage");
  }
  my $where=" WHERE 1=1";
  my $columns="queueId, status, name, execHost, submitHost ";
  my $all_status=0;
  my $error="";
  my $data;

  my @columns=(
	       {name=>"user", pattern=>"u(ser)?",
		start=>'submithost like \'',end=>"\@\%'"},
	       {name=>"host", pattern=>"h(ost)?",
		start=>'exechost like \'%\@',end=>"'"},
	       {name=>"submithost", pattern=>"submit(host)?",
		start=>'submithost like \'%\@',end=>"'"},
	       {name=>"id", pattern=>"i(d)?",
		start=>"queueid='",end=>"'"},
	       {name=>"split", pattern=>"s(plit)?",
		start=>"split='",end=>"'"},
	       {name=>"status", pattern=>"s(tatus)?",
		start=>"status='",end=>"'"},
	       {name=>"command", pattern=>"c(ommand)?",
		start=>"name='",end=>"'"},
	       {name=>"site", pattern=>"site",
		start=>"site='", end=>'\''}
	      );

  while (@_) {
    my $argv=shift;

    ($argv=~ /^-?-all_status=?/) and $all_status=1 and  next;
    ($argv=~ /^-?-a(ll)?=?/) and $columns.=", received, started, finished,split" 
      and next;
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
    $data->{$type} or $data->{$type}=[];

    push @{$data->{$type}}, "$found->{start}$value$found->{end}";
  }
  if ($error) {
    my $message="Error in top: $error\n$usage";
    $self->{LOGGER}->error("JobManager", $message);
    return (-1, $message);
  }

  foreach my $column (@columns){
    $data->{$column->{name}} or next;
    $where .= " and (".join (" or ", @{$data->{$column->{name}}} ).")";
  }
  $all_status or $data->{status} or $data->{id} or $where.=" and ( status='RUNNING' or status='WAITING' or status='OVER_WAITING' or status='ASSIGNED' or status='QUEUED' or status='INSERTING' or status='STARTED' or status='SAVING' or status='TO_STAGE' or status='STAGGING' or status='A_STAGED' or status='STAGING' or status='SAVED')";

  $where.=" ORDER by queueId";

  $self->info( "In getTop, doing query $columns, $where" );

  my $rresult = $self->{DB}->getFieldsFromQueueEx($columns, $where)
    or $self->{LOGGER}->error( "JobManager", "In getTop error getting data from database" )
      and return (-1, "error getting data from database");

  my @entries=@$rresult;
  $self->info( "Top done with $#entries +1");

  return $rresult;
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
  my $allparts = $self->{DB}->getFieldsFromQueueEx("count(*) as count, min(started) as started, max(finished) as finished, status", " WHERE $jobtag GROUP BY status");

  for (@$allparts) {
    $result->{$_->{status}} = $_->{count};
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
  my $allparts = $self->{DB}->getFieldsFromQueueEx("count(*) as count, status", "WHERE $jdljobtag GROUP BY status");
  
  my $userparts = $self->{DB}->getFieldsFromQueueEx("count(*) as count, status", "WHERE submitHost like '$username\@%' and $jdljobtag GROUP BY status");
  
  my $allsites  = $self->{DB}->getFieldsFromQueueEx("count(*) as count, site"," WHERE $jdljobtag Group by site");

  my $sitejobs =$self->{DB}->getFieldsFromQueueEx("count(*) as count, site, status", "WHERE $jdljobtag GROUP BY concat(site, status)");

  my $totalcost = $self->{DB}->queryRow("SELECT sum(cost) as cost FROM QUEUE WHERE $jdljobtag"); 

  my $totalusercost = $self->{DB}->queryRow("SELECT sum(cost) as cost FROM QUEUE WHERE submitHost like '$username\@%' and $jdljobtag");
  
  my $totalUsage = $self->{DB}->queryRow("SELECT sum(cpu*cpuspeed/100.0) as cpu,sum(rsize) as rmem,sum(vsize) as vmem FROM QUEUE WHERE status='RUNNING' and $jdljobtag");
  
  my $totaluserUsage = $self->{DB}->queryRow("SELECT sum(cpu*cpuspeed/100.0) as cpu,sum(rsize) as rmem,sum(vsize) as vmem FROM QUEUE WHERE submitHost like '$username\@%' and status='RUNNING' and $jdljobtag");
  
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
    my $type=lc($_->{status});
    $resultreturn->{"n$type"}=$_->{count};
  }

  for my $info (@$userparts) {
    foreach my $status (@{AliEn::Util::JobStatus()}) {
      if ($info->{status} eq lc($status)) {
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
	$site->{$site->{status}}=$_->{count};
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

=item GetJobJDL

returns the jdl of the job received as input

=cut

sub GetJobJDL {
  my $this=shift;
  my $id=shift;
 
 
  my $columns="jdl";
  my $method="queryValue"; 
  foreach my $o (@_) {
    $o=~ /-dir/ and $columns.=",path" and $method="queryRow";
    $o=~ /-status/ and $columns.=",status" and $method="queryRow";
  }

  $self->debug(1, "Asking for the jdl of $id");
  $id or $self->info( "No id to check in GetJOBJDL",11) and return (-1, "No id to check");
  my $rc=$self->{DB}->$method("select $columns from QUEUE where queueId=?", undef, {bind_values=>[$id]});
  $self->info( "Giving back the $columns of $id\n");
  return $rc; 

}


sub getTrace {
  my $this = shift;
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

sub getJobRc {
  my $this=shift;
  my $id=shift;

  $id or $self->info( "No id to check in getJobRc",11) and return (-1, "No id to check");
  my $rc=$self->{DB}->queryValue("select error from QUEUE where queueId=$id");
  $self->info( "The return code of $id is $rc\n");
  return $rc;
}

=item getPs

Gets the list of jobs from the queue

Possible flags:

=cut 

sub getPs {
  my $this = shift;
  my $flags= shift;
  my $args =join (" ", @_);
  my $date = time;
  my $i;

  my $status="status='RUNNING' or status='WAITING' or status='OVER_WAITING' or status='ASSIGNED' or status='QUEUED' or status='INSERTING' or status='SPLIT' or status='SPLITTING' or status='STARTED' or status='SAVING'";
  my $site="";

  $self->info( "Asking for ps (@_)..." );

  my $user="";
  my @userStatus;
  if ( $flags =~ s/d//g){
    push @userStatus, "status='DONE'";
  }
  if ( $flags =~ s/f//g){
    push @userStatus, "status='EXPIRED'", "status like 'ERROR_\%'",
      "status='KILLED'","status='FAILED'","status='ZOMBIE'";
  }
  if ( $flags=~ s/r//g) {
    push @userStatus,"status='RUNNING'", "status='SAVING'","status='WAITING'", "status='OVER_WAITING'", "status='ASSIGNED'", "status='QUEUED'", "status='INSERTING'", "status='SPLITTING'", "status='STARTED'", "status='SPLIT'";
  }
  
  if ( $flags =~ s/A//g) {
    push @userStatus, 1;
  }

  if ( $flags =~s/I//g) {
    push @userStatus,"status='IDLE'", "status='INTERACTIV'","status='FAULTY'";
  }
  if ( $flags=~ s/z//g) {
    push @userStatus,"status='ZOMBIE'";
  }

  while ($args =~ s/-?-s(tatus)? (\S+)//) {
    push @userStatus, "status='$1'";
  }
  @userStatus and $status=join (" or ", @userStatus) ;

  my @userSite;
  while ($args =~ s/-?-s(ite)? (\S+)//) {
    push @userSite, "site='$1'";
  }
  @userSite and $site="and ( ". join (" or ", @userSite). ")";

#    }
    #my $query="SELECT queueId, status, jdl, execHost, submitHost, runtime, cpu, mem, cputime, rsize, vsize, ncpu, cpufamily, cpuspeed, cost, maxrsize, maxvsize,received,started,finished  FROM QUEUE WHERE ( status=$status ) ";
  my $where = "WHERE ( $status ) $site ";


  $args =~ s/-?-u(ser)?=?\s+(\S+)// and $where.=" and submithost like '$2\@\%'";
#    $args =~ s/-?-e(xec)?=?\s+(\S+)// and $where.=" and exechost like '\%$2'";
#    $args =~ s/-?-c(ommand)?=?\s+(\S+)// and $where.=" and jdl like '\%Executable\%$2\%'";
    $args =~ s/-?-i(d)?=?\s*(\S+)// and $where .=" and ( p.queueid='$2' or split='$2')";

  if ($flags =~ s/s//) {
    $where .=" and (upper(jdl) like '\%SPLIT\%' or split>0 ) ";
  } elsif ($flags !~ s/S//) {
    $where .=" and ((split is NULL) or (split=0))";
  }

  if ($flags !~ /^\s*$/) {
    $self->info( "Error: I don't know what to do with '-$flags'");
    return (-1, "wrong syntax (don't know '-$flags')");

  }
  if ($args !~ /^\s*$/){
    $self->info( "Error: I don't know what to do with '$args'");
    return (-1, "wrong syntax (don't know '$args')");
  }

#    if ($args !~ /^\s*$/ ){
#      my $message="argument '$args' in ps not known";
#      $self->{LOGGER}->error("JobManager", "Error: $message");
#      return(-1, "$message");
#    }


#    my $query="SELECT queueId, status, jdl, execHost FROM QUEUE WHERE ( status=$status ) $user $exechost order by queueId";

  $where .=" and p.queueid=q.queueid ORDER BY q.queueId";

  $self->info( "In getPs getting data from database \n $where" );


	#my (@ok) = $self->{DB}->query($query);
  my $rresult = $self->{DB}->getFieldsFromQueueEx("q.queueId, status, jdl, execHost, submitHost, runtime, cpu, mem, cputime, rsize, vsize, ncpu, cpufamily, cpuspeed, cost, maxrsize, maxvsize, site, node, split, procinfotime,received,started,finished",
						  "q, QUEUEPROC p $where")
    or $self->{LOGGER}->error( "JobManager", "In getPs error getting data from database" )
      and return (-1, "error getting data from database");

  $DEBUG and $self->debug(1, "In getPs getting ps done" );

  my @jobs;
  for (@$rresult) {
    $DEBUG and $self->debug(1, "Found jobid $_->{queueId}");
    my ($executable) = $_->{jdl} =~ /.*Executable\s*=\s*"([^"]*)"/i;
    my ($split)      = $_->{jdl} =~ /.*Split\s*=.*"(.*)".*/i;
    $_->{cost} = int ($_->{cost});
    push @jobs, join ("###", $_->{queueId}, $_->{status}, $executable, $_->{execHost}, $_->{submitHost},
		      $_->{runtime}, $_->{cpu}, $_->{mem}, $_->{cputime}, $_->{rsize}, $_->{vsize},
		      $_->{ncpu}, $_->{cpufamily}, $_->{cpuspeed}, $_->{cost}, $_->{maxrsize}, $_->{maxvsize}, $_->{site},$_->{node},$split,$_->{split},$_->{procinfotime},$_->{received},$_->{started},$_->{finished});
  }

  (@jobs) or (push @jobs, "\n");

  $self->info( "ps done with $#jobs entries");

  return join ( "\n", @jobs );
}


sub getSpyUrl {
    my $this    = shift;
    my $queueId = shift  or return;
    $self->info("Get Spy Url for $queueId"); 
    my ($url)  = $self->{DB}->getFieldFromQueue($queueId,"spyurl");
    $url or $self->info("In spy cannot get the spyurl for job $queueId");
    $self->info("Returning Spy Url for $queueId $url"); 
    return $url;
}

sub queueinfo {
  my $this = shift;
  my $jdl="";
  grep (/^-jdl$/, @_) and $jdl="jdl,";
  @_=grep (!/^-jdl$/, @_);
  my $site = shift;
  my $sql="site,blocked, status, statustime,$jdl ". join(", ", @{AliEn::Util::JobStatus()});
  $self->info("Quering  $sql");
  my $array = $self->{DB}->getFieldsFromSiteQueueEx($sql,"where site like '$site' ORDER by site");
  (@$array) or return;


  return $array;
}

sub jobinfo {
  my $this = shift;
  my $site = shift or return;
  my $status = shift or return;
  my $delay = shift or return;
  my $now = time;
    
  my $array = $self->{DB}->getFieldsFromQueueEx("q.queueId","q, QUEUEPROC p where site like '$site' and status='$status' and ( ($now - procinfotime) > $delay) and q.queueid=p.queueid");

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



#sub GetJobJDL {
#    my $this = shift;
#    my $queueId = shift or return;
#
#    return $self->{DB}->getFieldFromQueue($queueId,"jdl");
#}



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





#/**************************************************************************
# * Copyright(c) 2001-2003, ALICE Experiment at CERN, All rights reserved. *
# *                                                                        *
# * Author: The ALICE Off-line Project / AliEn Team                        *
# * Contributors are mentioned in the code where appropriate.              *
# *                                                                        *
# * Permission to use, copy, modify and distribute this software and its   *
# * documentation strictly for non-commercial purposes is hereby granted   *
# * without fee, provided that the above copyright notice appears in all   *
# * copies and that both the copyright notice and this permission notice   *
# * appear in the supporting documentation. The authors make no claims     *
# * about the suitability of this software for any purpose. It is          *
# * provided "as is" without express or implied warranty.                  *
# **************************************************************************/

package AliEn::Database::TaskQueue;

use AliEn::Database;

use strict;
use AliEn::Util;

use vars qw(@ISA $DEBUG);
@ISA=("AliEn::Database");

$DEBUG=0;

sub preConnect {
  my $self=shift;
  $self->{DB} and $self->{HOST} and $self->{DRIVER} and return 1;

  $self->info("Using the default $self->{CONFIG}->{JOB_DATABASE}");
  ($self->{HOST}, $self->{DRIVER}, $self->{DB})
    =split ( m{/}, $self->{CONFIG}->{JOB_DATABASE});

  return 1;
}

sub initialize {
  my $self     = shift;

  $self->{QUEUETABLE}="QUEUE";
  $self->{SITEQUEUETABLE}="SITEQUEUES";
  $self->SUPER::initialize() or return;

  $self->{SKIP_CHECK_TABLES} and return 1;

  $self->setArchive();

  AliEn::Util::setupApMon($self);

  my $queueColumns={queueId=>"int(11) not null auto_increment primary key",
		    execHost=>"varchar(64)",
		    submitHost=>"varchar(64)",
		    priority =>"tinyint(4)",
		    status  =>"varchar(10)",
		    command =>"varchar(255)",
		    commandArg =>"varchar(255)",
		    name =>"varchar(255)",
		    path =>"varchar(255)",
		    current =>"varchar(255)",
		    received =>"int(20)",
		    started =>"int(20)",
		    finished =>"int(20)",
		    expires =>"int(10)",
		    error =>"int(11)",
		    validate =>"int(1)",
		    sent =>"int(20)",
		    jdl =>"text",
		    site=> "varchar(40)",
		    node=>"varchar(64)",
		    spyurl=>"varchar(64)",
		    split=>"int",
		    splitting=>"int",
		    merging=>"varchar(64)",
		    masterjob=>"int(1) default 0",
	            price=>"float",
	            effectivePriority=>"float",
	            finalPrice=>"float",
		    notify=>"varchar(255)",
		    agentid=>'int(11)'};
  my $tables={ QUEUE=>{columns=>$queueColumns,
		       id=>"queueId",
		       index=>"queueId",
		       extra_index=>["INDEX (split)", "INDEX (status)", "INDEX(agentid)"]},
	       QUEUEPROC=>{
			   columns=>{queueId=>"int(11) not null auto_increment primary key",
				     runtime =>"varchar(20)",
				     runtimes =>"int",
				     cpu =>"float",
				     mem =>"float",
				     cputime =>"int",
				     rsize =>"int",
				     vsize =>"int",
				     ncpu =>"int",
				     cpufamily =>"int",
				     cpuspeed =>"int",
				     cost =>"float",
				     maxrsize =>"float",
				     maxvsize =>"float",
				     procinfotime =>"int(20)",
				     si2k=>"float",
				    },
			   id=>"queueId",
			  },
	       QUEUEEXPIRED=>{columns=>$queueColumns,
		       id=>"queueId",
		       index=>"queueId"},	
	       $self->{QUEUEARCHIVE}=>{columns=>$queueColumns,
		       id=>"queueId",
		       index=>"queueId"},
	       JOBAGENT=>{columns=>{entryId=>"int(11) not null auto_increment primary key",
				    requirements=>"text not null",
				    counter=>"int(11) not null default 0",
				    afterTime=>"time",
				    beforeTime=>"time",
				    priority=>"int(11)",
				   },
			  id=>"entryId",
			  index=>"entryId",
			  extra_index=>["INDEX(priority)"],
			 },
	       SITES=>{columns=>{siteName=>"char(255)",
				 siteId =>"int(11) not null auto_increment primary key",
				 masterHostId=>"int(11)",
				 adminName=>"char(100)",
				 location=>"char(255)",
				 domain=>"char(30)",
				 longitude=>"float",
				 latitude=>"float",
				 record=>"char(255)",
				 url=>"char(255)",},
		       id=>"siteId",
		       index=>"siteId",
		       },
	       HOSTS=>{columns=>{commandName=>"char(255)",
				 hostName=>"char(255)",
				 hostPort=>"int(11) not null ",
				 hostId =>"int(11) not null auto_increment primary key",
				 siteId =>"int(11) not null",
				 adminName=>"char(100) not null",
				 maxJobs=>"int(11) not null",
				 status=>"char(10) not null",
				 date=>"int(11)",
				 rating=>"float not null",
				 Version=>"char(10)",
				 queues=>"char(50)",
				 connected=>"int(1)",
				 maxqueued=>"int(11)",
				},
		       id=>"hostId",
		       index=>"hostId"
		      },
	       MESSAGES=>{columns=>{ ID            =>" int(11) not null  auto_increment primary key",
				     TargetHost    =>" varchar(100)",
				     TargetService =>" varchar(100)",
				     Message       =>" varchar(100)",
				     MessageArgs   =>" varchar(100)",
				     Expires       =>" int(11)",
				     Ack=>         =>'varchar(255)'},
			  id=>"ID",
			  index=>"ID",},
	       JOBMESSAGES=>{columns=> {entryId=>" int(11) not null  auto_increment primary key",
					jobId =>"int", 
					procinfo=>"varchar(200)",
					tag=>"varchar(40)", 
					timestamp=>"int", },
			     id=>"entryId", 
			    },
	       BALANCE=>{ columns=> { ID        =>" INT(11) not null auto_increment primary key",
		                      groupName =>" VARCHAR(255) unique",
				      balance   =>" DOUBLE",},
		          id=>"ID",
			  index=>"ID",
			},	    
	       TRANSACTION=>{ columns=> { ID	    => " INT(11) not null auto_increment primary key",
			       		  fromGroup => " VARCHAR(255)",
					  toGroup   => " VARCHAR(255)",
					  amount    => " DOUBLE",
					  initiator => " VARCHAR(255)",
					  moment    => " TIMESTAMP",},
			      id=>"ID",
			      index=>"ID",
	           	    },
	       JOBSTOMERGE=>{columns=>{masterId=>"int(11) not null primary key"},
			     id=>"masterId"},
			
	     };

  foreach my $table  (keys %$tables) {
    $self->checkTable($table, $tables->{$table}->{id}, $tables->{$table}->{columns}, $tables->{$table}->{index}, $tables->{$table}->{extra_index})
      or $self->{LOGGER}->error("TaskQueue", "Error checking the table $table") and return;
  }

  $self->checkSiteQueueTable("SITEQUEUES")
    or $self->{LOGGER}->error( "TaskQueue", "In initialize altering tables failed for SITEQUEUES") 
		  and return;

  $self->checkPriorityTable("PRIORITY")
    or $self->{LOGGER}->error( "TaskQueue", "In initialize altering tables failed for PRIORITY")
      and return;

  $self->checkActionTable() or return;

  $self->{JOBLEVEL}->{'INSERTING'}   =10;
  $self->{JOBLEVEL}->{'SPLITTING'}   =15;
  $self->{JOBLEVEL}->{'SPLIT'}       =18;
  $self->{JOBLEVEL}->{'WAITING'}     =20;
  $self->{JOBLEVEL}->{'ASSIGNED'}    =25;
  $self->{JOBLEVEL}->{'QUEUED'}      =30;
  $self->{JOBLEVEL}->{'STARTED'}     =40;
  $self->{JOBLEVEL}->{'IDLE'}        =50;
  $self->{JOBLEVEL}->{'INTERACTIV'}  =50;
  $self->{JOBLEVEL}->{'RUNNING'}     =50;
  $self->{JOBLEVEL}->{'SAVING'}      =60;
  $self->{JOBLEVEL}->{'SAVED'}       =70;
  $self->{JOBLEVEL}->{'DONE'}        =980;
  $self->{JOBLEVEL}->{'ERROR_A'}     =990;
  $self->{JOBLEVEL}->{'ERROR_I'}     =990;
  $self->{JOBLEVEL}->{'ERROR_E'}     =990;
  $self->{JOBLEVEL}->{'ERROR_IB'}    =990;
  $self->{JOBLEVEL}->{'ERROR_M'}     =990;
  $self->{JOBLEVEL}->{'ERROR_R'}     =990;
  $self->{JOBLEVEL}->{'ERROR_S'}     =990;
  $self->{JOBLEVEL}->{'ERROR_SV'}    =990;
  $self->{JOBLEVEL}->{'ERROR_V'}     =990;
  $self->{JOBLEVEL}->{'ERROR_VN'}    =990;
  $self->{JOBLEVEL}->{'ERROR_VT'}    =990;
  $self->{JOBLEVEL}->{'ERROR_SPLT'}  =990;
  $self->{JOBLEVEL}->{'EXPIRED'}     =1000;
  $self->{JOBLEVEL}->{'FAILED'}      =1000;
  $self->{JOBLEVEL}->{'KILLED'}      =1001;

  $self->{JOBLEVEL}->{'FORCEMERGE'}  =950;

  $self->{JOBLEVEL}->{'MERGING'}     =970;


  $self->{JOBLEVEL}->{'ZOMBIE'}      =999;


  return 1;
}
sub setArchive{
  my $self= shift;
  my ($Second, $Minute, $Hour, $Day, $Month, $Year, $WeekDay, $DayOfYear, $IsDST) = localtime(time);
  $Year = $Year+1900;
  $self->{QUEUEARCHIVE} ="QUEUEARCHIVE".$Year;
}

sub setQueueTable{
  my $self = shift;
  $self->{QUEUETABLE} = (shift or "QUEUE");
}

sub checkActionTable {
  my $self=shift;

  my %columns= (action=>"char(40) not null primary key",
		todo=>"int(1) not null default 0");
  $self->checkTable("ACTIONS", "action", \%columns, "action") or return;
  return $self->do("INSERT IGNORE INTO ACTIONS(action) values  ('INSERTING'), ('MERGING'), ('KILLED'), ('SAVED'), ('SPLITTING')");
}

#sub insertValuesIntoQueue {
sub insertJobLocked {
  my $self = shift;
  my $set = {};
  $set->{jdl} = shift;
  $set->{received} = shift;
  $set->{status} = shift;
  $set->{submitHost} = shift;
  $set->{priority} = shift;
  $set->{split} = (shift or 0);
  my $oldjob = (shift or 0);
  ($set->{name}) = $set->{jdl}  =~ /.*executable\s*=\s*\"([^\"]*)\"/i;

  ($set->{price}) = $set->{jdl} =~ /.*price\s*=\s*(\d+[\.\d+]?)\s*/i;

   $set->{effectivePriority} = $set->{priority} * $set->{price};
   #currently $set->{priority} is hardcoded to be '0'
    
  $DEBUG and $self->debug(1, "In insertJobLocked locking the table $self->{QUEUETABLE}");
  $self->lock("$self->{QUEUETABLE} WRITE,QUEUEPROC");

  $DEBUG and $self->debug(1, "In insertJobLocked table $self->{QUEUETABLE} locked. Inserting new data.");
  $set->{jdl}=~ s/'/\\'/g;
  my $out = $self->insert("$self->{QUEUETABLE}",$set);
  
  my $procid="";
  ($out)
      and $procid = $self->getLastId();
  
  if ($procid){
    $DEBUG and $self->debug(1, "In insertJobLocked got job id $procid.");
    $self->insert("QUEUEPROC", {queueId=>$procid});
   }
  
  if ($oldjob!= 0) { 
    # remove the split master Id, since this job has been resubmitted ...
    my ($ok) =	$self->updateJob($oldjob,{split=>"0"});
    ($ok) or
      $self->{LOGGER}->error( "TaskQueue", "Update of resubmitted split job part failed!");
  }
  
  $DEBUG and $self->debug(1, "In insertJobLocked unlocking the table $self->{QUEUETABLE}.");	
  $self->unlock();
  my $action="INSERTING";
  $set->{jdl}=~ / split =/im and $action="SPLITTING";
  $self->update("ACTIONS", {todo=>1}, "action='$action'");
  $self->info("UPDATING $action and $set-{jdl}");
  # send the new job's status to ML
  $self->sendJobStatus($procid, 'INSERTING', "", $set->{submitHost});

  return $procid;
}

sub isWaiting{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("TaskQueue","In isWaiting job id is missing")
      and return;

  $DEBUG and $self->debug(1, "in isWaiting testing is job $id waiting");
  $self->queryValue("SELECT count(*) from $self->{QUEUETABLE} where status='WAITING' and queueId=?", undef, {bind_values=>[$id]});
}

sub assignWaiting{
  my $self = shift;

  my $queueID = shift
    or $self->{LOGGER}->error("TaskQueue","In assignWaiting job id is missing")
      and return;
  my $user = shift
    or $self->{LOGGER}->error("TaskQueue","In assignWaiting user is missing")
      and return;
  my $host = shift
    or $self->{LOGGER}->error("TaskQueue","In assignWaiting host is missing")
      and return;
  my $jdl  = shift;
  
  $jdl =~ /.*CE\s*=\s*\"(\S*)\".*/;
  my $ce = $1;
  $self->debug(1,"CE is $ce! jdl $jdl");

  $self->lock("$self->{QUEUETABLE} WRITE, QUEUEPROC");
  $DEBUG and $self->debug(1, "in assignWaiting table $self->{QUEUETABLE} locked");

  #Checking that the job is still waiting
  eval {
    $self->isWaiting($queueID) or die("the job '$queueID' is no longer waiting\n");
    $self->updateStatus($queueID,"%","ASSIGNED",
		      {sent=>time, execHost=>"$user\@$host", site=>"$ce"} ) 
      or die("error setting the job to 'ASSIGNED'\n");
  };
  my $error=$@;
  $self->unlock();

  if ($error) {
    $self->info( "Error assigning the job: $error");
    return ;
  }
  return 1;
}

sub updateQueue{
  my $self = shift;
  $self->update("$self->{QUEUETABLE}",@_);
}

sub deleteFromQueue{
  my $self = shift;
  $self->delete("$self->{QUEUETABLE}",@_);
}

#sub getWaitingJobs{
#  my $self = shift;
#  my $order = shift;
#  my $minpriority = (shift or "-128");
#  $order and $order = " ORDER BY $order" or $order = "";
#
#  $DEBUG and $self->debug(1, "In getWaitingJobs fetching attributes queueId,jdl for waiting jobs");
#  $self->query("SELECT queueId, jdl FROM $self->{QUEUETABLE} WHERE status='WAITING' AND jdl IS NOT NULL AND priority > ? $order", undef, {bind_values=>[$minpriority]});
#}

sub getWaitingJobAgents{
  my $self=shift;
  my $nocache=shift;

  if (! $nocache){
    my $list=AliEn::Util::returnCacheValue($self, "listWaitingJA");
    $list and return $list;
  }
  my $list=$self->query("select entryId as agentId,concat('[',requirements,'Type=\"Job\";TTL=999;]') as jdl, counter from JOBAGENT order by priority desc");

  if ($#$list >100){
    $nocache or AliEn::Util::setCacheValue($self, "listWaitingJA", $list);
  }
  return $list;
}
sub updateJob{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("TaskQueue","In updateJob job id is missing")
      and return;
  my $set =shift;
  my $opt=shift ||{};

  $DEBUG and $self->debug(1,"In updateJob updating job $id");
  my $procSet = {};
  foreach my $key (keys  %$set){
    if($key =~ /(si2k)|(cpuspeed)|(maxrsize)|(cputime)|(ncpu)|(cost)|(cpufamily)|(cpu)|(vsize)|(rsize)|(runtimes)|(procinfotime)|(maxvsize)|(runtime)|(mem)/){
      $procSet->{$key} = $set->{$key};
      delete $set->{$key};
    }
  }
  my $where="queueId=?";
  $opt->{where} and $where.=" and $opt->{where}";
  my @bind=($id);
  $opt->{bind_values} and push @bind, @{$opt->{bind_values}};
  my $done=$self->update($self->{QUEUETABLE}, $set,$where,{bind_values=>\@bind});

  #the update didn't work
  $done or return;
  #the update didn't modify any entries
  $done =~ /^0E0$/ and return;

  if(keys %$procSet){
    $self->update("QUEUEPROC", $procSet, "queueId=?", {bind_values=>[$id]}) or return;
  }
  return 1;
}

sub updateJobStats{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("TaskQueue","In updateJob job id is missing")
      and return;
  my $set =shift;

  $DEBUG and $self->debug(1,"In updateJob updating job $id");	
  $self->update("QUEUEPROC",$set,"queueId=?", {bind_values=>[$id]});
}

sub updateJobs{
  my $self = shift;
  my $set =shift;
  my @ids=@_;
  @ids or 
    $self->{LOGGER}->error("TaskQueue","In updateJobs job id is missing")
      and return;
  my $where="";
  foreach my $id (@ids){
    $where.=" queueId=? or";
  }
  $where =~ s/or$//;
#  map {$_=" queueId=$_ "} @ids;
#  my $where=join(" or ", @ids);
  $DEBUG and $self->debug(1,"In updateJob updating job $where");	
  $self->updateQueue($set,$where, {bind_values=>[@ids]});
}

sub deleteJob{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("TaskQueue","In deleteJob job id is missing")
      and return;
	
  $DEBUG and $self->debug(1,"In deleteJob updating job $id");	
  $self->deleteFromQueue("queueId=?", {bind_values=>[$id]});
}
#updateStatus
# This subroutine receives the ID and old status of a job, the new status and
# optionaly the jdl. If the job is still in the old status, it will change it. 
# Otherwise, it returns undef
# oldstatus could be '%'
sub updateStatus{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("TaskQueue","In updateStatus job id is missing")
      and return;
  my $oldstatus = shift
    or $self->{LOGGER}->error("TaskQueue","In updateStatus old status is missing")
      and return;
  my $status = shift;
  my $set = shift || {};
  #This is the service that will update the log of the job
  my $service=shift;

  $set->{status} = $status;
  $set->{procinfotime}=time;
	
  my $message="";

  $DEBUG and $self->debug(1, "In updateStatus checking if job $id with status $oldstatus exists");


  my $oldjobinfo = $self->getFieldsFromQueueEx("masterjob,site,execHost,status,agentid","where queueid=?", {bind_values=>[$id]});
  #Let's take the first entry
  $oldjobinfo and $oldjobinfo=shift @$oldjobinfo;
  if (!$oldjobinfo){
    $self->{LOGGER}->set_error_msg("The job $id was no longer in the queue");
    $self->info("There was an error: The job $id was no longer in the queue",1);
    return;
  }
  my $dbsite=$set->{site} || $oldjobinfo->{site} || "";
  my $execHost=$set->{execHost} || $oldjobinfo->{execHost};
  my $dboldstatus=$oldjobinfo->{status};
  my $masterjob=$oldjobinfo->{masterjob};
  my  $where="status = ?";

  if (($self->{JOBLEVEL}->{$status} <= $self->{JOBLEVEL}->{$dboldstatus} )
      && ($dboldstatus !~ /^((ZOMBIE)|(IDLE)|(INTERACTIV))$/ )
      && (! $masterjob)){
    my $message="The job $id [$dbsite] was in status $dboldstatus [$self->{JOBLEVEL}->{$dboldstatus}] and cannot be changed to $status [$self->{JOBLEVEL}->{$status}]";
    if ($set->{jdl} and $status =~/^(SAVED)|(ERROR_V)$/){
      $message.= " (although we update the jdl)";
      $self->updateJob($id, {jdl=>$set->{jdl}});
    }
    $self->{LOGGER}->set_error_msg("Error updating the job: $message");
    $self->info("Error updating the job: $message",1);
    return;
  }

  #update the value, it is correct
  if (!$self->updateJob($id,$set, {where=>"status=?", 
				   bind_values=>[$dboldstatus]}, ) ) {
    my $message="The update failed (the job changed status in the meantime??)";
    $self->{LOGGER}->set_error_msg($message);
    $self->info("There was an error: $message",1);
    return;
  }

  $self->info( "THE UPDATE WORKED!! Let's see if we have to delete an agent $status");
  ($status eq "ASSIGNED") and $oldjobinfo->{agentid} and 
    $self->deleteJobAgent($oldjobinfo->{agentid});
  # update the SiteQueue table
  # send the status change to ML
  $self->sendJobStatus($id, $status, $execHost, "");
  $status=~ /^(DONE)|(ERROR_.*)|(EXPIRED)|(KILLED)$/ and 
    $self->checkFinalAction($id, $service);
  if ( $status ne $oldstatus ) {
    if ( $status eq "ASSIGNED" ) {
      $self->info("In updateStatus increasing $status for $dbsite");
      $self->_do("UPDATE $self->{SITEQUEUETABLE} SET $status=$status+1 where site=?", {bind_values=>[$dbsite]}) or
	$message="TaskQueue: in update Site Queue failed";
    } else {
      $self->info("In updateStatus decreasing $dboldstatus and increasing $status for $dbsite");
      if (!$self->_do("UPDATE $self->{SITEQUEUETABLE} SET $dboldstatus = $dboldstatus-1, $status=$status+1 where site=?", {bind_values=>[$dbsite]})){
	$message="TaskQueue: in update Site Queue failed";
	$self->{LOGGER}->set_error_msg($message);
	$self->info("There was an error: $message",1);
	return;	
      }
    }
    ($status eq "KILLED") and 
      $self->update("ACTIONS", {todo=>1}, "action='KILLED'");
    ($status eq "SAVED") and 
      $self->update("ACTIONS", {todo=>1}, "action='SAVED'");
  }


  $DEBUG and $self->debug(1, "In updateStatus table $self->{QUEUETABLE} successfully unlocked");

  return 1;
}
sub checkFinalAction{
  my $self=shift;
  my $id=shift;
  my $service=shift;

  my $info = $self->queryRow("SELECT status,notify,split FROM QUEUE where queueid=?", undef, {bind_values=>[$id]}) or return;
  $self->info("Checking if we have to send an email for job $id...");  
  $info->{notify} and $self->sendEmail($info->{notify},$id, $info->{status}, $service);
  $self->info("Checking if we have to merge the master");
  if ($info->{split}){
    $self->info("We have to check if all the subjobs of $info->{split} have finished");
    $self->do("insert ignore into JOBSTOMERGE values (?)", {bind_values=>[$info->{split}]});
    $self->do("update ACTIONS set todo=1 where action='MERGING'");
  }
  return 1;
}


sub sendEmail{
  my $self=shift;
  my $address=shift;
  my $id=shift;
  my $status=shift;
  my $service=shift;

  $self->info("We are supposed to send an email!!! (status $status)");

  my $ua = new LWP::UserAgent;

  $ua->agent( "AgentName/0.1 " . $ua->agent );

#  my $message="The job produced the following files: $output\n
#You can get the output from the AliEn prompt typing:
#$type#
#
#You can also get the files from the shell prompt typing:
# 
#$shell";
#  $status=~ /^ERROR_/ and $message="The job did not run properly. This could be either a site being misconfigured\nYou can see the execution log in the AliEn prompt in the directory $procDir/job-log/execution.out\n";
  
  # Create a request
  my $req = HTTP::Request->new( POST => "mailto:$address" );
  $req->header(
		 Subject => "AliEn-Job $id finished with status $status" );
  my $URL=($self->{CONFIG}->{PORTAL_URL} || "http://alien.cern.ch/Alien/main?task=job&");
    $req->content("AliEn-Job $id finished with status $status\n
You can see the ouput produced by the job in ${URL}jobID=$id


Please, make sure to copy any file that you want, since those are temporary files, and will be deleted at some point.

If you have any problem, please contact us
"
		 );
  
  # Pass request to the user agent and get a response back
  
  my $res = $ua->request($req);
  if ($service) {
    $self->info("Let's put it in the job trace");
    $service->putJobLog($id, "trace", "Sending an email to $address (job $status)");
  }
  $self->info("ok");
  return 1;
}


sub setSplit {
  my $self = shift;
	
  $DEBUG and $self->debug(1,"In setSplit updating job's split");
  $self->updateJob(shift, {split=>shift});
}

sub setJdl {
  my $self = shift;
	
  $DEBUG and $self->debug(1,"In setJdl updating job's jdl");
  $self->updateJob(shift, {jdl=>shift});
}

sub getFieldFromQueue {
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("TaskQueue","In getFieldFromQueue job id is missing")
      and return;
  $id=~ /^[0-9]+$/ or $self->{LOGGER}->error("TaskQueue", "The id '$id' doesn't look like a job id") and return;
  my $attr = shift || "*";
  
  $DEBUG and $self->debug(1,"In getFieldFromQueue fetching attribute $attr of job $id");
  $self->queryValue("SELECT $attr FROM $self->{QUEUETABLE} WHERE queueId=?", undef, {bind_values=>[$id]});
}

sub getFieldsFromQueue {
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("TaskQueue","In getFieldsFromQueue job id is missing")
      and return;
  my $attr = shift || "*";

  $DEBUG and $self->debug(1,"In getFieldsFromQueue fetching attributes $attr of job $id");
  $self->queryRow("SELECT $attr FROM $self->{QUEUETABLE} WHERE queueId=?", undef, {bind_values=>[$id]});
}

sub getFieldsFromQueueEx {
  my $self = shift;
  my $attr = shift || "*";
  my $addsql = shift || "";
  
  $DEBUG and $self->debug(1,"In getFieldsFromQueueEx fetching attributes $attr with condition $addsql from table $self->{QUEUETABLE}");
  $self->query("SELECT $attr FROM $self->{QUEUETABLE} $addsql", undef, @_);
}

sub getFieldFromQueueEx {
  my $self = shift;
  my $attr = shift || "*";
  my $addsql = shift || "";
  
  $DEBUG and $self->debug(1,"In getFieldFromQueueEx fetching attributes $attr with condition $addsql from table $self->{QUEUETABLE}");
  $self->queryColumn("SELECT $attr FROM $self->{QUEUETABLE} $addsql", undef, @_);
}

sub getProofSites{
	my $self = shift;
    
	#make debug message a bit more understandable later
	$DEBUG and $self->debug(1,"In getProofSites fetching sites with jobs with proofd command");     
	$self->queryColumn("select distinct site from $self->{QUEUETABLE} where jdl like ?", undef, {bind_values=>['%Command::PROOFD%/bin/proofd%']});
}

sub getNumberAvailableProofs{
	my $self = shift;
	my $site = shift
		or $self->{LOGGER}->error("TaskQueue","In getNumberAvailableProofs site is missing")
		and return;
	
	#make debug message a bit more understandable later
	$DEBUG and $self->debug(1,"In getNumberAvailableProofs fetching count of jobs on site $site with proofd command and status IDLE");     
	$self->queryValue("select count(*) from $self->{QUEUETABLE} where jdl like ? and status=? and site=?", undef, {bind_values=>['%Command::PROOFD%/bin/proofd%', 'IDLE', $site]});
}

sub getNumberBusyProofs{
	my $self = shift;
	my $site = shift
		or $self->{LOGGER}->error("TaskQueue","In getNumberBusyProofs site is missing")
		and return;
	
	#make debug message a bit more understandable later
	$DEBUG and $self->debug(1,"In getNumberBusyProofs fetching count of jobs on site $site with proofd command and status INTERACTIV");     
	$self->queryValue("select count(*) from $self->{QUEUETABLE} where jdl like ? and status=? and site=?", undef, {bind_values=>['%Command::PROOFD%/bin/proofd%', 'INTERACTIV', $site]});
}

sub getJobsByStatus {
  my $self = shift;
  my $status = shift
    or $self->{LOGGER}->error("TaskQueue","In getJobsByStatus status is missing")
      and return;
  my $order = shift || "";
  
  $order and $order = " ORDER BY $order";
  
  $DEBUG and $self->debug(1,"In getJobsByStatus fetching jobs with status $status");     
  $self->query("SELECT queueid,jdl from $self->{QUEUETABLE} where status='$status' $order");
}

#sub setStatusSplitting {
#	my $self = shift;
#	my $id = shift;#
#
#	$self->lock("$self->{QUEUETABLE}");#
#
#	if ($self->queryRow("SELECT count(*) from $self->{QUEUETABLE} where status='WAITING' and queueid=$id")) {
#		$self->updateStatus($id,"SPLITTING")
#			or print STDERR "TaskQueue: in setStatusSplitting updating of status failed"
#			and return;
#    }
#	else {
#		$self->info( "The job was not waiting any more...");
#		return -1;
#	}#
#
#	$self->unlock();#
#
#	1;
#}

### HOSTS

sub updateHosts{
  shift->update("HOSTS",@_);
}

sub deleteFromHosts{
  shift->update("HOSTS",@_);
}

sub insertHost{
  shift->insert("HOSTS",@_);
}

sub updateHost{
  my $self = shift;
  my $hostname = shift
    or $self->{LOGGER}->error("TaskQueue","In updateHost host name is missing")
      and return;
  my $set =shift;

  $DEBUG and $self->debug(1,"In updateHost updating host $hostname");	
  $self->updateHosts($set,"hostname=?", {bind_values=>[$hostname]});
}

sub insertHostSiteId {
	my $self = shift;
	my $host = shift;
	my $domainId = shift;

	$DEBUG and $self->debug(1,"In insertHostSiteId inserting new host with data: host=".($host or "")." and siteId=".($domainId or ""));
	$self->insertHost({hostName=>$host, siteId=>$domainId});
}

sub getMaxJobsMaxQueued{
	my $self = shift;

	$DEBUG and $self->debug(1,"In getMaxJobsMaxQueued fetching host name, maxjobs and maxqueued for connected or active hosts");
	$self->query("SELECT hostname,maxjobs,maxqueued FROM HOSTS WHERE status='CONNECTED' OR status='ACTIVE'");
}

sub getFieldFromHosts {
	my $self = shift;
	my $hostName = shift
		or $self->{LOGGER}->error("TaskQueue","In getFieldFromHosts host name is missing")
		and return;
	my $attr = shift || "*";
	
	$DEBUG and $self->debug(1,"In getFieldFromHosts fetching attribute $attr of host $hostName");
	$self->queryValue("SELECT $attr FROM HOSTS WHERE hostName=?", undef, {bind_values=>[$hostName]});
}

sub getFieldsFromHosts{
	my $self = shift;
	my $hostName = shift
		or $self->{LOGGER}->error("TaskQueue","In getFieldsFromHosts host name is missing")
		and return;
	my $attr = shift || "*";

	$DEBUG and $self->debug(1,"In getFieldsFromHosts fetching attributes $attr of host $hostName");
	$self->queryRow("SELECT $attr FROM HOSTS WHERE hostName=?", undef, {bind_values=>[$hostName]});
}

sub getFieldsFromHostsEx {
	my $self = shift;
	my $attr = shift || "*";
	my $addsql = shift || "";
	
	$DEBUG and $self->debug(1,"In getFieldsFromHostsEx fetching attributes $attr with condition $addsql from table HOSTS");
	$self->query("SELECT $attr FROM HOSTS $addsql", undef, @_);
}

sub getFieldFromHostsEx {
	my $self = shift;
	my $attr = shift || "*";
	my $addsql = shift || "";
	
	$DEBUG and $self->debug(1,"In getFieldFromHostsEx fetching attributes $attr with condition $addsql from table HOSTS");
	$self->queryColumn("SELECT $attr FROM HOSTS $addsql", undef, @_);
}

###		SITES

sub getSitesByDomain{
	my $self = shift;
	my $domain = shift
		or $self->{LOGGER}->error("TaskQueue","In getSitesByDomain domain is missing")
		and return;
	my $attr = shift || "*";

	$DEBUG and $self->debug(1,"In getSitesByDomain fetching attributes $attr for domain $domain");	
	$self->query("SELECT $attr FROM SITES where domain=?", undef, {bind_values=>[$domain]});
}

sub insertSite{
	shift->insert("SITES",@_);
}

sub updateSites{
	shift->update("SITES",@_);
}

sub deleteFromSites{
	shift->delete("SITES",@_);
}

###		MESSAGES

sub insertMessage{
	shift->insert("MESSAGES",@_);
}

sub updateMessages{
	shift->update("MESSAGES",@_);
}

sub deleteFromMessages{
	shift->update("MESSAGES",@_);
}

###		runs

sub updateRunsCol{
	my $self = shift;
	my $run = shift
		or $self->{LOGGER}->error("TaskQueue","In updateRunsCol run is missing")
		and return;
	my $round = shift
		or $self->{LOGGER}->error("TaskQueue","In updateRunsCol round is missing")
		and return;
	my $column =shift || "failed";

	$DEBUG and $self->debug(1,"In updateRunsCol decreasing value of column $column by 1 where run=$run and round=$round");
	return $self->do("UPDATE runs set $column=$column-1 WHERE run=? and round=?", {bind_values=>[$run, $round]});
}

###             SITEQUEUE
sub setSiteQueueTable{
    my $self = shift;
    $self->{SITEQUEUETABLE} = (shift or "SITEQUEUES");
}

#
# This subroutine puts all the columns of the SiteQueue table to 0;

sub resetSiteQueue {
  my $self=shift;

  #Let's put all the columns to 0
  my $ini={};
  foreach my $s (@{AliEn::Util::JobStatus()}){
    $ini->{$s}=0;
  }
  $self->updateSiteQueue($ini);
}


sub resyncSiteQueueTable{
  my $self = shift;
  $self->info("Extracting all sites from the QUEUE ....");
  my $allsites = $self->getFieldsFromQueueEx("site"," Group by site");
  @$allsites or $self->info("Warning: at the moment there are no sites defined in your organization")
    and return 1;

  my $site;
  my $now = time;
  my $qstat;

  $self->resetSiteQueue();

  foreach (@$allsites) {

    my $siteName=(defined $_->{'site'} ? $_->{site} : "undef");
    $self->info("Doing site $siteName");

    my $set={};
    $set->{'cost'} = 0;
    $set->{'status'}     = "resync";
    $set->{'statustime'} = "$now";
    foreach my $sstat (@{AliEn::Util::JobStatus()}) {
      $set->{$sstat}=0;
    }
    # query all job status;
    my $where="WHERE ";
    (defined $_->{site}) and  $where.="site='$_->{site}'" or 
      $where.="site is NULL";

    my $sitestat = $self->getFieldsFromQueueEx("status, sum(cost) as cost, count(*) as count","q, QUEUEPROC p $where and p.queueid=q.queueid Group by status");
    # delete all entries 
    #	$self->deleteSiteQueue("site='$_->{'site'}'");
    # loop over all status;
    foreach $qstat (@$sitestat) {
      my $logit = sprintf "Putting status %-10s for site %-40s to %-5s", $qstat->{status}, $siteName, $qstat->{count};
      $self->info($logit);

      $set->{$qstat->{status}}=$qstat->{count};
      $qstat->{'cost'} and  $set->{'cost'} += $qstat->{'cost'};
    }
    $_->{'site'}  or $_->{'site'} = "UNASSIGNED::SITE";

    $self->info("check for $_->{'site'}");
    my $exists = $self->getFieldsFromSiteQueueEx("site","WHERE site=?", {bind_values=>[$_->{site}]});
    if ( @$exists ) {
      $self->updateSiteQueue($set, "site=?", {bind_values=>[$_->{site}]});
    } else {
      $set->{'site'} = "$_->{site}";
      $set->{'blocked'} = "open";
      foreach (@{AliEn::Util::JobStatus()}) {
	if($_ eq "") {next;}
	$set->{$_} = 0;
      }
      $self->insertSiteQueue($set) or return;
    }
  }
  return 1;
}

sub checkSiteQueueTable{
  my $self = shift;
  $self->{SITEQUEUETABLE} = (shift or "SITEQUEUES");

  my %columns = (		
		 site=> "varchar(40) not null",
		 cost=>"float",
		 status=>"varchar(20)",
		 statustime=>"int(20)",
		 blocked =>"varchar(10)",
		 maxqueued=>"int",
		 maxrunning=>"int",
		 queueload=>"float",
		 runload=>"float",
		 jdl => "text",
		 timeblocked=>"datetime", 
		);

  foreach (@{AliEn::Util::JobStatus()}) {
    $columns{$_}="int";
  }
  $self->checkTable($self->{SITEQUEUETABLE}, "site", \%columns, "site");
}

sub setSiteQueueStatus {
  my $self = shift;
  my $site = shift or return;
  my $status = shift or return;
  my $jdl  =shift ||"";;
  my $set={};
  $set->{site}=$site;
  $set->{status} = "$status";
  $set->{statustime} = time;
  $jdl and $set->{jdl}=$jdl;

  my $done=$self->updateSiteQueue($set,"site=?", {bind_values=>[$site]});
  if ( $done =~ /^0E0$/){
    $self->insertSiteQueue($set);
  }
}

sub deleteSiteQueue{
    my $self = shift;
    $self->delete("$self->{SITEQUEUETABLE}",@_);
}
sub updateSiteQueue{
    my $self = shift;
    $self->update("$self->{SITEQUEUETABLE}",@_);
}

sub insertSiteQueue{
    my $self = shift;
    $self->insert("$self->{SITEQUEUETABLE}",@_);
}

sub getFieldFromSiteQueue {
	my $self = shift;
	my $site = shift
		or $self->{LOGGER}->error("TaskQueue","In getFieldFromSiteQueue site name is missing")
		and return;
	my $attr = shift || "*";
	
	$DEBUG and $self->debug(1,"In getFieldFromSiteQueue fetching attribute $attr of site $site");
	$self->queryValue("SELECT $attr FROM $self->{SITEQUEUETABLE} WHERE site=?", undef, {bind_values=>[$site]});
}

sub getFieldsFromSiteQueue {
	my $self = shift;
	my $site = shift
		or $self->{LOGGER}->error("TaskQueue","In getFieldsFromSiteQueue site name is missing")
		and return;
	my $attr = shift || "*";
	
	$DEBUG and $self->debug(1,"In getFieldsFromSiteQueue fetching attributes $attr of site name");
	$self->queryRow("SELECT $attr FROM $self->{SITEQUEUETABLE} WHERE site=?", undef, {bind_values=>[$site]});
}

sub getFieldsFromSiteQueueEx {
	my $self = shift;
	my $attr = shift || "*";
	my $addsql = shift || "";
	
	$DEBUG and $self->debug(1,"In getFieldsFromSiteQueueEx fetching attributes $attr with condition $addsql from table $self->{SITEQUEUETABLE}");
	$self->query("SELECT $attr FROM $self->{SITEQUEUETABLE} $addsql", undef, @_);
}

sub getFieldFromSiteQueueEx {
	my $self = shift;
	my $attr = shift || "*";
	my $addsql = shift || "";
	
	$DEBUG and $self->debug(1,"In getFieldFromSiteQueueEx fetching attributes $attr with condition $addsql from table $self->{SITEQUEUETABLE}");
	$self->queryColumn("SELECT $attr FROM $self->{SITEQUEUETABLE} $addsql", undef, @_);
}

###     Priority table
sub setPriorityTable{
  my $self = shift;
  $self->{PRIORITYTABLE} = (shift or "PRIORITY");
}

sub checkPriorityTable{
  my $self = shift;
  $self->{PRIORITYTABLE} = (shift or "PRIORITY");

  my %columns = (	
		 user=>"varchar(64) not null",
			priority=>"float",
			maxparallelJobs=>"int",
			nominalparallelJobs=>"int",
			computedpriority=>"float",
			waiting=>"int",
			running=>"int",
			userload=>"float"
		);

  $self->checkTable($self->{PRIORITYTABLE}, "user", \%columns, 'user');
  
}

#sub checkPriorityValue() {
#    my $self = shift;
#    my $user = shift or $self->{LOGGER}->error("TaskPriority","no username provided in checkPriorityValue");
#    my $exists = $self->getFieldFromPriority("$user");
#    if ($exists) {
#	$self->info( "$user entry for priority is existing!" );
#    } else {
#	$self->info( "$user entry for priority is not existing!" );
#	my $set = {};
#	$set->{'user'} = "$user";
#	$set->{'priority'} = "1.0";
#	$set->{'maxparallelJobs'} = 20;
#	$set->{'nominalparallelJobs'} = 10;
#	$set->{'computedpriority'} = 1;
#	$set->{'running'} = 0;
#	$set->{'waiting'} = 0;
#	$set->{'userload'} = 0;
#	$self->insertPrioritySet($user,$set);
#    }
#}


#sub insertPriority{
#    my $self = shift;
#    $self->insert("$self->{PRIORITYTABLE}",@_);
#}

#sub updatePriority{
#    my $self = shift;
#    $self->update("$self->{PRIORITYTABLE}",@_);
#}

#sub deletePriority{
#    my $self = shift;
#    $self->delete("$self->{PRIORITYTABLE}",@_);
#}

#sub insertPrioritySet{
#	my $self = shift;
#	my $user = shift
#		or $self->{LOGGER}->error("TaskPriority","In insertPrioritySet user is missing")
#		and return;
#	my $set =shift;
#	
#	$DEBUG and $self->debug(1,"In insertPrioritySet user is missing");
#	$self->insertPriority($set,"user='$user'");
#}

#sub updatePrioritySet{
#	my $self = shift;
#	my $user = shift
#		or $self->{LOGGER}->error("TaskPriority","In updatePrioritySet user is missing")
#		and return;
#	my $set =shift;
#	
#	$DEBUG and $self->debug(1,"In updatePrioritySet user is missing");
#	$self->updatePriority($set,"user='$user'");
#}

#sub deletePrioritySet{
#	my $self = shift;
#	my $user = shift
#		or $self->{LOGGER}->error("TaskPriority","In deletePrioritySet user is missing")
#		and return;
#	
#	$DEBUG and $self->debug(1,"In deletePrioritySet deleting user $user");	
#	$self->deleteFromPriority("user='$user'");
#}

#sub getFieldFromPriority {
#	my $self = shift;
#	my $user = shift
#		or $self->{LOGGER}->error("TaskPriority","In getFieldFromPriority user is missing")
#		and return;
#	my $attr = shift || "*";
#	
#	$DEBUG and $self->debug(1,"In getFieldFromPriority fetching attribute $attr of user $user");
#	$self->queryValue("SELECT $attr FROM $self->{PRIORITYTABLE} WHERE user='$user'");
#}

#sub getFieldsFromPriority {
#  my $self = shift;
#  my $user = shift
#    or $self->{LOGGER}->error("TaskPriority","In getFieldsFromPriority user is missing")
#      and return;
#  my $attr = shift || "*";
#	
#  $DEBUG and $self->debug(1,"In getFieldsFromPriority fetching attributes $attr of user $user");
#  $self->queryRow("SELECT $attr FROM $self->{PRIORITYTABLE} WHERE user='$user'");
#}

#sub getFieldsFromPriorityEx {
#  my $self = shift;
#  my $attr = shift || "*";
#  my $addsql = shift || "";
#	
#  $DEBUG and $self->debug(1,"In getFieldsFromPriorityEx fetching attributes $attr with condition $addsql from table $self->{PRIORITYTABLE}");
#  $self->query("SELECT $attr FROM $self->{PRIORITYTABLE} $addsql");
#}

#sub getFieldFromPriorityEx {
#  my $self = shift;
#  my $attr = shift || "*";
#  my $addsql = shift || "";
#	
#  $DEBUG and $self->debug(1,"In getFieldFromPriorityEx fetching attributes $attr with condition $addsql from table $self->{PRIORITYTABLE}");
#  $self->queryColumn("SELECT $attr FROM $self->{PRIORITYTABLE} $addsql");
#}



###     QUEUE Copy 
sub insertEntry{
    my $self = shift;
    my $dsttable = (shift or return);
    my $href = ( shift or return);
    my $newhash ={};

    my $queue = $self->describeTable("$dsttable");

    defined $queue
	or return;
    
    foreach (@$queue){
	if ( ($_->{Field} ne "" ) )
	{
	    $newhash->{$_->{Field}} = $href->{$_->{Field}};
	}
    }

    $DEBUG and $self->debug(1,"Copy Entry to $dsttable");
    $self->insert($dsttable, $newhash);
}

#### JobAgent
sub insertJobAgent {
  my $self=shift;
  my $text=shift;

  $text=~ s/\s*$//s;
  $self->info( "Inserting a jobagent with '$text'");
  $self->lock("JOBAGENT");
  my $id=$self->queryValue("SELECT entryId from JOBAGENT where requirements=?", undef, {bind_values=>[$text]});
  if (!$id){
    if (!$self->insert("JOBAGENT", {counter=>"1", requirements=>$text})){
      $self->info("Error inserting the new jobagent");
      $self->unlock();
      return;
    }
    $id=$self->getLastId();
  }else{
    $self->do("UPDATE JOBAGENT set counter=counter+1 where entryId=?", {bind_values=>[$id]});
  }
  $self->unlock();
  return $id;
}

sub deleteJobAgent {
  my $self=shift;
  my $id=shift;
  $self->info( "Deleting a jobagent for '$id'");
  my $done=$self->do("update JOBAGENT set counter=counter-1 where entryId=?", {bind_values=>[$id]});
  $self->delete("JOBAGENT", "counter<1");
  return $done;
}



# send a job's status to MonaLisa
sub sendJobStatus {
  my $self = shift;
  my ($jobID, $newStatus, $execHost, $submitHost) = @_;

  if($self->{MONITOR}){
    my $statusID = AliEn::Util::statusForML($newStatus);
    $execHost = $execHost || "NO_SITE";
    my @params = ("jobID", $jobID, "statusID", $statusID);
    push(@params, "submitHost", "$jobID/$submitHost") if $submitHost;
    $self->{MONITOR}->sendParameters("TaskQueue_Jobs_".$self->{CONFIG}->{ORG_NAME}, $execHost, @params);
  }
}

sub retrieveJobMessages {
  my $self=shift;
  my $time=time;
  my $info=$self->query("SELECT * from JOBMESSAGES where timestamp < ?", undef, {bind_values=>[$time]});
  $self->delete("JOBMESSAGES", "timestamp < ?", {bind_values=>[$time]});
  return $info;
}

sub insertJobMessage {
  my $self=shift;
  my $jobId=shift;
  my $tag=shift; 
  my $message=shift;
  my $time=time;
  return $self->insert("JOBMESSAGES", {jobId=>$jobId, procinfo=>$message,
			     tag=>$tag,  timestamp=>$time});

}




=head1 NAME

AliEn::Database::TaskQueue

=head1 DESCRIPTION

The AliEn::Database::TaskQueue module extends AliEn::Database module. Module
contains method specific for tables from database processes.

=head1 SYNOPSIS

  use AliEn::Database::TaskQueue;

  my $dbh = AliEn::Database::TaskQueue->new($dbOptions);

  $res = $dbh->getFieldFromQueue($jobId, $attr);
  $hashRef = $dbh->getFieldsFromQueue($jobId, $attr);
  $arrRef = $dbh->getFieldFromQueueEx($attr, $addSql);
  $arrRef = $dbh->getFieldsFromQueueEx($attr, $addSql);
  
  $res = $dbh->getFieldFromHosts($hostName, $attr);
  $hashRef = $dbh->getFieldsFromHosts($hostName, $attr);
  $arrRef = $dbh->getFieldFromHostsEx($attr, $addSql);
  $arrRef = $dbh->getFieldsFromHostsEx($attr, $addSql);
  
  $arrRef = $dbh->getProofSites();
  $res = $dbh->getNumberAvailableProofs($site);
  $res = $dbh->getNumberBusyProofs($site);
  $arrRef = $dbh->getJobsByStatus($status, $orderBy);
  
  $arrRef = $dbh->getMaxJobsMaxQueued();
  
  $res = $dbh->isWaiting($jobId);
  
  $res = $dbh->insertHost($insertSet);
  $res = $dbh->insertMessage($insertSet);
  $res = $dbh->insertSite($insertSet);
  
  $res = $dbh->insertJobLocked($jdl, $received, $status, $submitHost, $priority);
  $res = $dbh->insertHostSiteId($host, $siteId);
  
  $res = $dbh->updateQueue($updateSet, $where);
  $res = $dbh->updateHosts($updateSet, $where);
  $res = $dbh->updateSites($updateSet, $where);
  $res = $dbh->updateMessages($updateSet, $where);
    
  $res = $dbh->updateJob($id, $set);
  $res = $dbh->updateHost($hostname, $set);
  $res = $dbh->updateStatus($jobId, $oldStatus, $newStatus, $set);
  $res = $dbh->assignWaiting($jobId, $user, $host);  
  
  $res = $dbh->setSplit($jobId, $split);
  $res = $dbh->setJdl($jobId, $jdl);
    
  $res = $dbh->updateRunsCol($run, $round, $column);

  $res = $dbh->deleteFromQueue($where);
  $res = $dbh->deleteFromHosts($where);
  $res = $dbh->deleteFromMessages($where);
  $res = $dbh->deleteFromHosts($where);
  
  $res = $dbh->deleteJob($jobId);

=cut

=head1 METHODS

=over

=item C<new>

  $dbh = AliEn::Database::Transfer->new( $attr );

  $dbh = AliEn::Database::Transfer->new( $attr, $attrDBI );

Creates new AliEn::Database::Transfer instance. Arguments are passed to AliEn::Database
method new. For details about arguments see AliEn::Database method C<new>.

=item C<getFieldFrom*>

  $res = $dbh->getFieldFromQueue($jobId, $attr);

  $res = $dbh->getFieldFromHosts($hostName, $attr);  
  
Method fetches value of attribute $attr for tuple with defined unique id: 
in case of Queue job id and in case of Hosts hostname.
If unique id is not defined method will return undef and report error.
Method calls AliEn::Database metod queryValue.

=item C<getFieldsFrom*>

  $hashRef = $dbh->getFieldsFromQueue($jobId, $attr);

  $hashRef = $dbh->getFieldsFromHosts($hostName, $attr);  

Method fetches set of attributes $attr for tuple with defined unique id: 
in case of Queue job id and in case of Hosts hostname.
Result is reference to hash. Keys in hash are identical to names of attriutes 
in $attr set.
If set of attributes is not defined method returns values of all attributes. If
unique id is not defined method will return undef and report error.
Method calls AliEn::Database metod queryRow.

=item C<getFieldFrom*Ex>

  $arrRef = $dbh->getFieldFromQueueEx($attr, $addSql);
  
  $arrRef = $dbh->getFieldFromHostsEx($attr, $addSql);

Method fetches value of attribute $attr for tuples with condition $addSql.
Argument $addSql contains anything that comes after FROM part of SELECT statement.
Method returns reference to array which contains values of attribute $attr.
If $addSql condition is not defined method will return all tuples.
Method calls AliEn::Database metod queryColumn.

=item C<getFieldsFrom*Ex>

  $arrRef = $dbh->getFieldsFromQueueEx($attr, $addSql);
  
  $arrRef = $dbh->getFieldsFromHostsEx($attr, $addSql);

Method fetches set of attributes $attr for tuples with with condition $addSql.
Argument $addSql contains anything that comes after FROM part of SELECT statement.
If set of attributes is not defined method returns values of all attributes.
If $addSql condition is not defined method will return all tuples.
Method calls AliEn::Database metod query.

=item C<getProofSites>  

  $arrRef = $dbh->getProofSites();
  
Method returns list of sites which contain jobs with proof command. Return value
is reference to array which contains site names.
Method uses AliEn::Database method queryColumn. 

=item C<getNumberAvailableProofs>  

  $res = $dbh->getNumberAvailableProofs($site);
  
Method returns number of jobs with proof command and status IDLE for site $site. 
Method uses AliEn::Database method queryValue. 

=item C<getNumberBusyProofs>  

  $res = $dbh->getNumberBusyProofs($site);
  
Method returns number of jobs with proof command and status INTERACTIV for site $site. 
Method uses AliEn::Database method queryValue. 

=item C<getJobsByStatus>  

  $arrRef = $dbh->getJobsByStatus($status, $orderBy);
  
Method returns values of attributes queuueId and jdl for tuples with status
$status. If argument $status is not defined method will return undef and 
report error. Argument $orderBy is ORDER BY part of SELECT query. 
Method uses AliEn::Database method query. 

=item C<getMaxJobsMaxQueued>  

  $arrRef = $dbh->getMaxJobsMaxQueued();
  
Method returns values of attributes hostname, maxjobs and maxqueued for
tuples from table HOSTS with status CONNECTED or ACTIVE.
Method uses AliEn::Database method query. 

=item C<isWaiting>  

  $res = $dbh->isWaiting($jobId);
  
Method checks if job with job id $jobId is in status WAITING. 
If $jobId is not defined method will return undef and report error.

=item C<insert*> 

  $res = $dbh->insertHost($insertSet);
  
  $res = $dbh->insertSite($insertSet);
  
  $res = $dbh->insertMessage(insertSet);
  
Method just calls AliEn::Database method C<insert>. Method defines table argument
and passes $insertSet and $where arguments to AliEn::Database C<insert> method. 

=item C<insertJobLocked>  

  $res = $dbh->insertJobLocked($jdl, $received, $status, $submitHost, $priority);
  
Method inserts new job with defined arguments. Before inserting method
locks table $self->{QUEUETABLE} and after inserting unlocks the table.   

=item C<insertHostSiteId>     
   
  $res = $dbh->insertHostSiteId($host, $siteId);

Method inserts new tuple into table HOSTS with defined attributes host
and site ID.

=item C<update*> 

  $res = $dbh->updateQueue($updateSet, $where);
  
  $res = $dbh->updateHosts($updateSet, $where);
  
  $res = $dbh->updateSites($updateSet, $where);
  
  $res = $dbh->updateMessages($updateSet, $where);

Method just calls AliEn::Database method C<update>. Method defines table argument
and passes $updateSet and $where arguments to AliEn::Database C<update> method. 

=item C<updateJob>     

  $res = $dbh->updateJob($id, $set);
  
Method updates job with id $jobId with update set $set. Form of 
$set argument is defined in AliEn::Database C<update> method.  
If job id is not defined method will return undef and report error.

=item C<updateHost>     

  $res = $dbh->updateHost($hostname, $set);
  
Method updates host with host name $hostname with update set $set. Form of 
$set argument is defined in AliEn::Database C<update> method.  
If job id is not defined method will return undef and report error.
  
=item C<updateStatus> 
  
  $res = $dbh->updateStatus($jobId, $oldStatus, $newStatus);
  
  $res = $dbh->updateStatus($jobId, $oldStatus, $newStatus, $set);
  
Method checkes if job with id $jobId and status $oldStatus exists.
If job exists method updates job's status to $newStatus. If argument
$oldStatus is set to "%" method will ignore old status of job $jobId.
Method can update other attributes if argument $set is defined. Form of 
$set argument is defined in AliEn::Database C<update> method.
If arguments $jobId or $oldStatus are not defined method will return undef
and report error.
  
=item C<assignWaiting>   
  
  $res = $dbh->assignWaiting($jobId, $user, $host);

Method checkes if job $jobId is in WAITING state. If job is in WAITING state 
method will set it's state to ASSIGNED, sent attribute to current time and execHost
attribute to '$user@$host'. 
Before doing any operation method locks table TRANSFER and unlocks it at the end.

=item C<set*>     

  $res = $dbh->setSplit($jobId, $split);
  
  $res = $dbh->setJdl($jobId, $jdl);
  
Method updates attribute for job with id $jobId. If job id is not
defined method will return undef and report error.
  
=item C<updateRunsCol>   

  $res = $dbh->updateRunsCol($run, $round, $column);  
  
Method decreases value of attribute $column by 1 for tuples from table runs
with value of run $run and value of round $round. If $run or $round are not 
defined method will return undef and report error. Default $column is finished.

=item C<deleteFrom*> 

  $res = $dbh->deleteFromQueue($where);
  
  $res = $dbh->deleteFromHost($where);
  
  $res = $dbh->deleteFromSite($where);
  
  $res = $dbh->deleteFromMessage($where);

Method just calls AliEn::Database method C<update>. Method defines table argument
and passes $updateSet and $where arguments to AliEn::Database C<update> method. 

=item C<deleteJob>     

  $res = $dbh->deleteJob($id);
  
Method deletes job with id $id.
If job id is not defined method will return undef and report error.

=back

=head1 SEE ALSO

AliEn::Database

=cut

1;

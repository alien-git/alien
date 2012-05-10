#
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
@ISA = ("AliEn::Database");

$DEBUG = 0;

sub preConnect {
  my $self = shift;
  $self->{DB} and $self->{HOST} and $self->{DRIVER} and return 1;

  $self->info("Using the default $self->{CONFIG}->{JOB_DATABASE}");
  ($self->{HOST}, $self->{DRIVER}, $self->{DB}) =
    split(m{/}, $self->{CONFIG}->{JOB_DATABASE});

  return 1;
}

sub initialize {
  my $self = shift;

  $self->{QUEUETABLE}     = "QUEUE";
  $self->{SITEQUEUETABLE} = "SITEQUEUES";
  $self->{JOBTOKENTABLE} = "JOBTOKEN";
  $self->{PRIORITYTABLE} = "PRIORITY";
  $self->SUPER::initialize() or return;

  $self->{JOBLEVEL} = {
    'INSERTING'    => 10,
    'SPLITTING'    => 15,
    'SPLIT'        => 18,
    'TO_STAGE'     => 16,
    'A_STAGED'     => 17,
    'STAGING'      => 19,
    'WAITING'      => 20,
    'OVER_WAITING' => 21,
    'ASSIGNED'     => 25,
    'QUEUED'       => 30,
    'STARTED'      => 40,
    'IDLE'         => 50,
    'INTERACTIV'   => 50,
    'RUNNING'      => 50,
    'SAVING'       => 60,
    'SAVED'        => 70,
    'DONE'         => 980,
    'SAVED_WARN'   => 71,
    'DONE_WARN'    => 981,
    'ERROR_A'      => 990,
    'ERROR_I'      => 990,
    'ERROR_E'      => 990,
    'ERROR_IB'     => 990,
    'ERROR_M'      => 990,
    'ERROR_RE'     => 990,
    'ERROR_S'      => 990,
    'ERROR_SV'     => 990,
    'ERROR_V'      => 990,
    'ERROR_VN'     => 990,
    'ERROR_VT'     => 990,
    'ERROR_SPLT'   => 990,
    'EXPIRED'      => 1000,
    'FAILED'       => 1000,
    'KILLED'       => 1001,
    'FORCEMERGE'   => 950,

    'MERGING' => 970,
    'ZOMBIE'  => 999,
    'ERROR_EW'     => 990
  };

  if ($self->{CONFIG}->{JOB_DATABASE_READ}) {
    $self->info("Connecting to $self->{CONFIG}->{JOB_DATABASE_READ} for the select queries");
    my $options = {};
    foreach my $key (keys %$self) {
      $options->{$key} = $self->{$key};
    }
    ($options->{HOST}, $options->{DRIVER}, $options->{DB}) =
      split("/", $self->{CONFIG}->{JOB_DATABASE_READ});

    $self->{DB_READ} = AliEn::Database->new($options) or return;
  }

  $self->{SKIP_CHECK_TABLES} and return 1;

  $self->setArchive();

  AliEn::Util::setupApMon($self);

  my $queueColumns = {
    columns => {
      queueId      => "int(11) not null auto_increment primary key",
      execHost     => "varchar(64)",
      submitHost   => "varchar(64)",
      priority     => "tinyint(4)",
      status       => "varchar(12)",
      name         => "varchar(255)",
      path         => "varchar(255)",
      received     => "int(20)",
      started      => "int(20)",
      finished     => "int(20)",
      expires      => "int(10)",
      error        => "int(11)",
      validate     => "int(1)",
      sent         => "int(20)",
      site         => "varchar(40)",
      node         => "varchar(64)",
      spyurl       => "varchar(64)",
      split        => "int",
      splitting    => "int",
      merging      => "varchar(64)",
      masterjob    => "int(1) default 0",
      price        => "float",
      chargeStatus => "varchar(20)",
      optimized    => "int(1) default 0",
      finalPrice   => "float",
      notify       => "varchar(255)",
      agentid      => 'int(11)',
      mtime        => 'timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
      resubmission => 'int(11) not null default 0',
    },
    id          => "queueId",
    index       => "queueId",
    extra_index => [
      "INDEX (split)",
      "INDEX (status)",
      "INDEX(agentid)",
      "UNIQUE INDEX (submitHost,queueId)",
      "INDEX(priority)",
      "INDEX (site,status)",
      "INDEX (sent)",
      "INDEX (status,submitHost)",
      "INDEX (status,agentid)",
      "UNIQUE INDEX (status,queueId)"
    ],
    engine =>'innodb'
  };
  my $queueColumnsProc = {
    columns => {
      queueId      => "int(11) not null",
      runtime      => "varchar(20)",
      runtimes     => "int",
      cpu          => "float",
      mem          => "float",
      cputime      => "int",
      rsize        => "int",
      vsize        => "int",
      ncpu         => "int",
      cpufamily    => "int",
      cpuspeed     => "int",
      cost         => "float",
      maxrsize     => "float",
      maxvsize     => "float",
      procinfotime => "int(20)",
      si2k         => "float",
      lastupdate   => "timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
      batchid      => "varchar(255)",
    },
    id    => "queueId",
    extra_index=> ['foreign key (queueid) references QUEUE(queueid) on delete cascade'],
    engine =>'innodb'
  };
  my $queueColumnsJDL ={
  	columns=>{    
  		queueId      => "int(11) not null",
  		origJdl      =>"text collate latin1_general_ci",
  		resultsJdl          => "text collate latin1_general_ci",
  		
  	},
  	id =>"queueId",
  	extra_index=> ['foreign key (queueid) references QUEUE(queueid) on delete cascade'],
    engine =>'innodb'
  };
  
  # new for jobToken migration from ADMIN
  my $queueColumnsJobtoken ={
  	columns=>{    
		"jobId"    => "int(11)  DEFAULT '0' NOT NULL",
		"userName" => "char(20) DEFAULT NULL",
		"jobToken" => "char(255) DEFAULT NULL",	
  	},
  	id =>"jobId",
  	extra_index=> ['foreign key (jobId) references QUEUE(queueid) on delete cascade'],
    engine =>'innodb'
  };
  
  my $tables = {
    QUEUE            => $queueColumns,
    QUEUEPROC        => $queueColumnsProc,
    QUEUEJDL         => $queueColumnsJDL,
    JOBTOKEN    => $queueColumnsJobtoken,
    
    $self->{QUEUEARCHIVE}     => $queueColumns,
    $self->{QUEUEARCHIVEPROC} => $queueColumnsProc,
    JOBAGENT => {
      columns => {
        entryId      => "int(11) not null auto_increment primary key",
        counter      => "int(11)   default 0 not null ",
        priority     => "int(11)",
        ttl          => "int(11)",
        site         => "varchar(50) COLLATE latin1_general_ci",
        packages     => "varchar(255) COLLATE latin1_general_ci",
        disk         => "int(11)",
        partition    => "varchar(50) COLLATE latin1_general_ci",
        ce           => "varchar(50) COLLATE latin1_general_ci",
        user         => "varchar(12)",
        fileBroker    => "tinyint(1) default 0 not null",
      },
      id          => "entryId",
      index       => "entryId",
      extra_index => [ "INDEX(priority)", "INDEX(ttl)" ],
    },
    SITES => {
      columns => {
        siteName     => "char(255)",
        siteId       => "int(11) not null auto_increment primary key",
        masterHostId => "int(11)",
        adminName    => "char(100)",
        location     => "char(255)",
        domain       => "char(30)",
        longitude    => "float",
        latitude     => "float",
        record       => "char(255)",
        url          => "char(255)",
      },
      id    => "siteId",
      index => "siteId",
    },
    ##this table used to have several columns that could not be null. This fails when starting the
    ##cluster monitor. Indeed, null values are inserted. So we allow these columns to be nullable.
    HOSTS => {
      columns => {
        hostName  => "char(255)",
        hostPort  => "int(11) ",
        hostId    => "int(11) not null auto_increment primary key",
        siteId    => "int(11) not null",
        maxJobs   => "int(11)",
        status    => "char(10) ",
        date      => "int(11)",
        rating    => "float",
        Version   => "char(10)",
        queues    => "char(50)",
        connected => "int(1)",
        maxqueued => "int(11)",
        cename    => "varchar(255)",
      },
      id    => "hostId",
      index => "hostId"
    },
    MESSAGES => {
      columns => {
        ID            => " int(11) not null  auto_increment primary key",
        TargetHost    => " varchar(100)",
        TargetService => " varchar(100)",
        Message       => " varchar(100)",
        MessageArgs   => " varchar(100)",
        Expires       => " int(11)",
        Ack           => => 'varchar(255)'
      },
      id    => "ID",
      index => "ID",
    },
    JOBMESSAGES => {
      columns => {
        entryId   => " int(11) not null  auto_increment primary key",
        jobId     => "int",
        procinfo  => "varchar(200)",
        tag       => "varchar(40)",
        timestamp => "int",
      },
      id => "entryId",
    },

    JOBSTOMERGE => {
      columns => {masterId => "int(11) not null primary key"},
      id      => "masterId"
    },
    STAGING => {
      columns => {
        queueid      => "int(11) not null primary key",
        staging_time => "timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
      },
      id => "queueid"
    },
    FILES_BROKER=> {
    	columns=>{"lfn"=> "varchar(255) not null",
    			"split" =>"int(11) not null ",
    			"sites"=>"varchar(255) not null",
    			"queueid" => "int(11) default null",
    	},

    	extra_index=>['index(split)', "unique index(split,lfn)"],
    	id=>"lfn"
    	
    }

  };
  foreach my $table (keys %$tables) {
    $self->checkTable(
      $table,
      $tables->{$table}->{id},
      $tables->{$table}->{columns},
      $tables->{$table}->{index},
      $tables->{$table}->{extra_index}
      )
      or $self->{LOGGER}->error("TaskQueue", "Error checking the table $table")
      and return;
  }

  $self->checkSiteQueueTable("SITEQUEUES")
    or $self->{LOGGER}->error("TaskQueue", "In initialize altering tables failed for SITEQUEUES")
    and return;

  $self->checkActionTable() or return;
  
  # PRIORITY TABLE
  $self->checkPriorityTable()
    or $self->{LOGGER}->error("TaskQueue", "In initialize altering tables failed for PRIORITY")
    and return;
  

  return 1;
}

sub setArchive {
  my $self = shift;
  my ($Second, $Minute, $Hour, $Day, $Month, $Year, $WeekDay, $DayOfYear, $IsDST) = localtime(time);
  $Year                     = $Year + 1900;
  $self->{QUEUEARCHIVE}     = "QUEUEARCHIVE" . $Year;
  $self->{QUEUEARCHIVEPROC} = "QUEUEARCHIVE" . $Year . "PROC";
}

sub setQueueTable {
  my $self = shift;
  $self->{QUEUETABLE} = (shift or "QUEUE");
}

sub checkActionTable {
  my $self = shift;

  my %columns = (
    action => "char(40) not null primary key",
    todo   => "int(1)  default 0 not null "
  );
  $self->checkTable("ACTIONS", "action", \%columns, "action") or return;
  $self->do(
"INSERT  INTO ACTIONS(action)  (SELECT 'INSERTING' from dual where not exists (select action from ACTIONS where action like 'INSERTING'))"
    )
    and $self->do(
"INSERT  INTO ACTIONS(action)  (SELECT 'MERGING' from dual where not exists (select action from ACTIONS where action like 'MERGING'))"
    )
    and $self->do(
"INSERT  INTO ACTIONS(action)  (SELECT 'KILLED' from dual where not exists (select action from ACTIONS where action like 'KILLED'))"
    )
    and $self->do(
"INSERT  INTO ACTIONS(action)  (SELECT 'SPLITTING' from dual where not exists (select action from ACTIONS where action like 'SPLITTING'))"
    )
    and $self->do(
"INSERT  INTO ACTIONS(action)  (SELECT 'STAGING' from dual where not exists (select action from ACTIONS where action like 'STAGING'))"
    )
    and $self->do(
"INSERT  INTO ACTIONS(action)  (SELECT 'SAVED' from dual where not exists (select action from ACTIONS where action like 'SAVED'))"
    )
    and $self->do(
"INSERT  INTO ACTIONS(action)  (SELECT 'SAVED_WARN' from dual where not exists (select action from ACTIONS where action like 'SAVED_WARN'))"
    ) and return 1;

}

#sub insertValuesIntoQueue {
sub insertJobLocked {
  my $self = shift;
  my $set  = shift
    or $self->info("Error getting the job to insert")
    and return;
  my $oldjob = (shift or 0);

  $set->{received} = time;
  ($set->{name}) = $set->{jdl} =~ /.*executable\s*=\s*\"([^\"]*)\"/i;

  my ($tmpPrice) = $set->{jdl} =~ /.*price\s*=\s*(\d+.*)\;\s*/i;
  $tmpPrice = sprintf("%.3f", $tmpPrice);
  $set->{price} = $tmpPrice;

  $set->{chargeStatus} = 0;

  #currently $set->{priority} is hardcoded to be '0'

  $DEBUG and $self->debug(1, "In insertJobLocked table $self->{QUEUETABLE} locked. Inserting new data.");
  $set->{jdl} =~ s/'/\\'/g;
  my $jdl=$set->{jdl};
  delete $set->{jdl};
  my $out = $self->insert("$self->{QUEUETABLE}", $set);

  my $procid = "";
  ($out)
    and $procid = $self->getLastId($self->{QUEUETABLE});

  if ($procid) {
    $DEBUG and $self->debug(1, "In insertJobLocked got job id $procid.");
    $self->insert("QUEUEPROC", {queueId => $procid});
    $self->insert("QUEUEJDL", {queueId =>$procid, origJdl=>$jdl})
  }

  if ($oldjob != 0) {

    # remove the split master Id, since this job has been resubmitted ...
    my ($ok) = $self->updateJob($oldjob, {split => "0"});
    ($ok)
      or $self->{LOGGER}->error("TaskQueue", "Update of resubmitted split job part failed!");
  }

  $DEBUG
    and $self->debug(1, "In insertJobLocked unlocking the table $self->{QUEUETABLE}.");

  my $action = "INSERTING";
  $jdl =~ / split =/im and $action = "SPLITTING";
  ($set->{status} !~ /WAITING/)
    and $self->update("ACTIONS", {todo => 1}, "action='$action'");

  # send the new job's status to ML
  $self->sendJobStatus($procid, $set->{status}, "", $set->{submitHost});

  return $procid;
}


sub updateQueue {
  my $self = shift;
  $self->update("$self->{QUEUETABLE}", @_);
}

sub deleteFromQueue {
  my $self = shift;
  $self->delete("$self->{QUEUETABLE}", @_);
}

#

sub updateJob {
  my $self = shift;
  my $id   = shift
    or $self->info( "In updateJob job id is missing")
    and return;
  my $set = shift;
  my $opt = shift || {};

  $DEBUG and $self->debug(1, "In updateJob updating job $id");
  my $procSet = {};
  my $jdlSet  = {};
  foreach my $key (keys %$set) {
    if ($key =~
/(si2k)|(cpuspeed)|(maxrsize)|(cputime)|(ncpu)|(cost)|(cpufamily)|(cpu)|(vsize)|(rsize)|(runtimes)|(procinfotime)|(maxvsize)|(runtime)|(mem)|(batchid)/
      ) {
      $procSet->{$key} = $set->{$key};
      delete $set->{$key};
    }elsif ($key =~ /(\S*)jdl/){
    	$jdlSet->{$key}=$set->{$key};
    	delete $set->{$key};
    }
    
  }
  my $where = "queueId=?";
  $opt->{where} and $where .= " and $opt->{where}";
  my @bind = ($id);
  $opt->{bind_values} and push @bind, @{$opt->{bind_values}};
  my $done = $self->update($self->{QUEUETABLE}, $set, $where, {bind_values => \@bind});

  #the update didn't work
  $done or return;

  #the update didn't modify any entries
  $done =~ /^0E0$/ and return;

  if (keys %$procSet) {
    $self->update("QUEUEPROC", $procSet, "queueId=?", {bind_values => [$id]})
      or return;
  }
  if (keys %$jdlSet) {
  	$self->update("QUEUEJDL", $jdlSet, "queueId=?", {bind_values=>[$id]}) or return;
  	
  }
  return 1;
}

sub updateJobStats {
  my $self = shift;
  my $id   = shift
    or $self->{LOGGER}->error("TaskQueue", "In updateJob job id is missing")
    and return;
  my $set = shift;

  $DEBUG and $self->debug(1, "In updateJob updating job $id");
  $self->update("QUEUEPROC", $set, "queueId=?", {bind_values => [$id]});
}

sub updateJobs {
  my $self = shift;
  my $set  = shift;
  my @ids  = @_;
  @ids
    or $self->{LOGGER}->error("TaskQueue", "In updateJobs job id is missing")
    and return;
  my $where = "";
  foreach my $id (@ids) {
    $where .= " queueId=? or";
  }
  $where =~ s/or$//;

  #  map {$_=" queueId=$_ "} @ids;
  #  my $where=join(" or ", @ids);
  $DEBUG and $self->debug(1, "In updateJob updating job $where");
  $self->updateQueue($set, $where, {bind_values => [@ids]});
}

sub deleteJob {
  my $self = shift;
  my $id   = shift
    or $self->{LOGGER}->error("TaskQueue", "In deleteJob job id is missing")
    and return;

  $DEBUG and $self->debug(1, "In deleteJob updating job $id");
  $self->deleteFromQueue("queueId=?", {bind_values => [$id]});
}

#updateStatus
# This subroutine receives the ID and old status of a job, the new status and
# optionaly the jdl. If the job is still in the old status, it will change it.
# Otherwise, it returns undef
# oldstatus could be '%'
sub updateStatus {
  my $self = shift;
  my $id   = shift
    or $self->{LOGGER}->error("TaskQueue", "In updateStatus job id is missing")
    and return;
  my $oldstatus = shift
    or $self->{LOGGER}->error("TaskQueue", "In updateStatus old status is missing")
    and return;
  my $status = shift;
  my $set = shift || {};

  #This is the service that will update the log of the job
  my $service = shift;

  $set->{status}       = $status;
  $set->{procinfotime} = time;

  my $message = "";

  $DEBUG
    and $self->debug(1, "In updateStatus checking if job $id with status $oldstatus exists");

  my $oldjobinfo =
    $self->getFieldsFromQueueEx("masterjob,site,execHost,status,agentid", "where queueid=?", {bind_values => [$id]});

  #Let's take the first entry
  $oldjobinfo and $oldjobinfo = shift @$oldjobinfo;
  if (!$oldjobinfo) {
    $self->{LOGGER}->set_error_msg("The job $id was no longer in the queue");
    $self->info("There was an error: The job $id was no longer in the queue", 1);
    return;
  }
  my $dbsite = $set->{site} || $oldjobinfo->{site} || "";
  my $execHost = $set->{execHost} || $oldjobinfo->{execHost};
  my $dboldstatus = $oldjobinfo->{status};
  my $masterjob   = $oldjobinfo->{masterjob};
  my $where       = "status = ?";

  if ( ($self->{JOBLEVEL}->{$status} <= $self->{JOBLEVEL}->{$dboldstatus})
    && ($dboldstatus !~ /^((ZOMBIE)|(IDLE)|(INTERACTIV))$/)
    && (!$masterjob)) {
    if ($set->{path}) {
      return $self->updateJob($id, {path => $set->{path}});
    }
    my $message =
"The job $id [$dbsite] was in status $dboldstatus [$self->{JOBLEVEL}->{$dboldstatus}] and cannot be changed to $status [$self->{JOBLEVEL}->{$status}]";
    if ($set->{jdl} and $status =~ /^(SAVED)|(SAVED_WARN)|(ERROR_V)$/) {
      $message .= " (although we update the jdl)";
      $self->updateJob($id, {jdl => $set->{jdl}});
    }
    $self->{LOGGER}->set_error_msg("Error updating the job: $message");
    $self->info("Error updating the job: $message", 1);
    return;
  }

  #update the value, it is correct
  if (!$self->updateJob($id, $set, {where => "status=?", bind_values => [$dboldstatus]},)) {
    my $message = "The update failed (the job changed status in the meantime??)";
    $self->{LOGGER}->set_error_msg($message);
    $self->info("There was an error: $message", 1);
    return;
  }

  $self->info("THE UPDATE WORKED!! Let's see if we have to delete an agent $status");
  if ($dboldstatus =~ /WAITING/ and $oldjobinfo->{agentid}) {
    $self->deleteJobAgent($oldjobinfo->{agentid});
  }

  # update the SiteQueue table
  # send the status change to ML
  $self->sendJobStatus($id, $status, $execHost, "");
  $status =~ /^(DONE.*)|(ERROR_.*)|(EXPIRED)|(KILLED)$/
    and $self->checkFinalAction($id, $service);
  if ($status ne $oldstatus) {
    if ($status eq "ASSIGNED") {
      $self->info("In updateStatus increasing $status for $dbsite");
      $self->_do("UPDATE $self->{SITEQUEUETABLE} SET $status=$status+1 where site=?", {bind_values => [$dbsite]})
        or $message = "TaskQueue: in update Site Queue failed";
    } else {
      $self->info("In updateStatus decreasing $dboldstatus and increasing $status for $dbsite");
      if (
        !$self->_do(
          "UPDATE $self->{SITEQUEUETABLE} SET $dboldstatus = $dboldstatus-1, $status=$status+1 where site=?",
          {bind_values => [$dbsite]}
        )
        ) {
        $message = "TaskQueue: in update Site Queue failed";
        $self->{LOGGER}->set_error_msg($message);
        $self->info("There was an error: $message", 1);
      }
    }

    $status =~ /^(KILLED)|(SAVED)|(SAVED_WARN)|(STAGING)$/
      and $self->update("ACTIONS", {todo => 1}, "action='$status'");
  }
  if ($status =~ /^DONE_WARN$/) {
    $self->sendJobStatus($id, "DONE", $execHost, "");
  }

  $DEBUG
    and $self->debug(1, "In updateStatus table $self->{QUEUETABLE} successfully unlocked");

  return 1;
}

sub checkFinalAction {
  my $self    = shift;
  my $id      = shift;
  my $service = shift;

  my $info =
    $self->queryRow("SELECT submitHost,status,notify,split FROM QUEUE where queueid=?", undef, {bind_values => [$id]})
    or return;
  $self->info("Checking if we have to send an email for job $id...");
  $info->{notify}
    and $self->sendEmail($info->{notify}, $id, $info->{status}, $service, $self->{submitHost});
  $self->info("Checking if we have to merge the master");
  if ($info->{split}) {
    $self->info("We have to check if all the subjobs of $info->{split} have finished");
    $self->do(
"insert  into JOBSTOMERGE (masterId) select ? from DUAL  where not exists (select masterid from JOBSTOMERGE where masterid = ?)",
      {bind_values => [ $info->{split}, $info->{split} ]}
    );
    $self->do("update ACTIONS set todo=1 where action='MERGING'");
  }
  return 1;
}

sub sendEmail {
  my $self       = shift;
  my $address    = shift;
  my $id         = shift;
  my $status     = shift;
  my $service    = shift;
  my $submitHost = shift;

  $self->info("We are supposed to send an email!!! (status $status)");

  my $ua = new LWP::UserAgent;

  $ua->agent("AgentName/0.1 " . $ua->agent);

  my $procdir = AliEn::Util::getProcDir(undef, $submitHost, $id);

#  my $message="The job produced the following files: $output\n
#You can get the output from the AliEn prompt typing:
#$type#
#
#You can also get the files from the shell prompt typing:
#
#$shell";
#  $status=~ /^ERROR_/ and $message="The job did not run properly. This could be either a site being misconfigured\nYou can see the execution log in the AliEn prompt in the directory $procDir/job-log/execution.out\n";

  # Create a request
  my $req = HTTP::Request->new(POST => "mailto:$address");
  $req->header(Subject => "AliEn-Job $id finished with status $status");
  my $URL = ($self->{CONFIG}->{PORTAL_URL} || "http://alien.cern.ch/Alien/main?task=job&");
  $req->content(
    "AliEn-Job $id finished with status $status\n
If the job created any output, you can find it in the alien directory $procdir/job-output


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

  $DEBUG and $self->debug(1, "In setSplit updating job's split");
  $self->updateJob(shift, {split => shift});
}

sub setJdl {
  my $self = shift;

  $DEBUG and $self->debug(1, "In setJdl updating job's jdl");
  $self->updateJob(shift, {jdl => shift});
}

sub getFieldFromQueue {
  my $self = shift;
  my $id   = shift
    or $self->{LOGGER}->error("TaskQueue", "In getFieldFromQueue job id is missing")
    and return;
  $id =~ /^[0-9]+$/
    or $self->{LOGGER}->error("TaskQueue", "The id '$id' doesn't look like a job id")
    and return;
  my $attr = shift || "*";

  $DEBUG
    and $self->debug(1, "In getFieldFromQueue fetching attribute $attr of job $id");
  $self->queryValue("SELECT $attr FROM $self->{QUEUETABLE} WHERE queueId=?", undef, {bind_values => [$id]});
}

sub getFieldsFromQueue {
  my $self = shift;
  my $id   = shift
    or $self->{LOGGER}->error("TaskQueue", "In getFieldsFromQueue job id is missing")
    and return;
  my $attr = shift || "*";

  $DEBUG
    and $self->debug(1, "In getFieldsFromQueue fetching attributes $attr of job $id");
  my $join="";
  $attr =~ /jdl/ and $join = "join QUEUEJDL using (queueId)";
  $self->queryRow("SELECT $attr FROM $self->{QUEUETABLE} $join WHERE queueId=?", undef, {bind_values => [$id]});
}

sub getFieldsFromQueueEx {
  my $self   = shift;
  my $attr   = shift || "*";
  my $addsql = shift || "";

  $DEBUG
    and $self->debug(1,
    "In getFieldsFromQueueEx fetching attributes $attr with condition $addsql from table $self->{QUEUETABLE}");
  if ($self->{DB_READ}) {
    $self->info("Retrieving the info from the read database!!");
    return $self->{DB_READ}->query("SELECT $attr FROM $self->{QUEUETABLE} $addsql", undef, @_);
  }
  $self->query("SELECT $attr FROM $self->{QUEUETABLE} $addsql", undef, @_);
}

sub getFieldFromQueueEx {
  my $self   = shift;
  my $attr   = shift || "*";
  my $addsql = shift || "";

  $DEBUG
    and $self->debug(1,
    "In getFieldFromQueueEx fetching attributes $attr with condition $addsql from table $self->{QUEUETABLE}");
  $self->queryColumn("SELECT $attr FROM $self->{QUEUETABLE} $addsql", undef, @_);
}


sub getJobsByStatus {
  my $self   = shift;
  my $status = shift
    or $self->{LOGGER}->error("TaskQueue", "In getJobsByStatus status is missing")
    and return;
  my $order = shift || "";
  my $f     = shift;
  my $limit = shift;
  my $minid = shift || 0;

  #We never want to get more tahn 15 jobs at the same time, just in case the jdls are too long
  $order and $order = " ORDER BY $order";
  my $bind = [];
  if ($minid) {
    $order = " and queueid>? $order";
    push @$bind, $minid;
  }
  my $query = "SELECT queueid,ifnull(resultsjdl, origjdl) jdl from $self->{QUEUETABLE} join QUEUEJDL using (queueid) where status='$status' $order";
  $query = $self->paginate($query, $limit, 0);

  $DEBUG
    and $self->debug(1, "In getJobsByStatus fetching jobs with status $status");
  $self->query($query, undef, {bind_values => $bind});
}


### HOSTS

sub updateHosts {
  shift->update("HOSTS", @_);
}

sub deleteFromHosts {
  shift->update("HOSTS", @_);
}

sub insertHost {
  shift->insert("HOSTS", @_);
}

sub updateHost {
  my $self     = shift;
  my $hostname = shift
    or $self->{LOGGER}->error("TaskQueue", "In updateHost host name is missing")
    and return;
  my $set = shift;

  $DEBUG and $self->debug(1, "In updateHost updating host $hostname");
  $self->updateHosts($set, "hostname=?", {bind_values => [$hostname]});
}

sub insertHostSiteId {
  my $self     = shift;
  my $host     = shift;
  my $domainId = shift;

  $DEBUG
    and $self->debug(1,
    "In insertHostSiteId inserting new host with data: host=" . ($host or "") . " and siteId=" . ($domainId or ""));
  $self->insertHost({hostName => $host, siteId => $domainId});
}

sub getMaxJobsMaxQueued {
  my $self = shift;

  $DEBUG
    and
    $self->debug(1, "In getMaxJobsMaxQueued fetching host name, maxjobs and maxqueued for connected or active hosts");
  $self->query("SELECT hostname,maxjobs,maxqueued,cename FROM HOSTS WHERE status='CONNECTED' OR status='ACTIVE'");
}

sub getFieldFromHosts {
  my $self     = shift;
  my $hostName = shift
    or $self->{LOGGER}->error("TaskQueue", "In getFieldFromHosts host name is missing")
    and return;
  my $attr = shift || "*";

  $DEBUG
    and $self->debug(1, "In getFieldFromHosts fetching attribute $attr of host $hostName");
  $self->queryValue("SELECT $attr FROM HOSTS WHERE hostName=?", undef, {bind_values => [$hostName]});
}

sub getFieldsFromHosts {
  my $self     = shift;
  my $hostName = shift
    or $self->{LOGGER}->error("TaskQueue", "In getFieldsFromHosts host name is missing")
    and return;
  my $attr = shift || "*";

  $DEBUG
    and $self->debug(1, "In getFieldsFromHosts fetching attributes $attr of host $hostName");
  $self->queryRow("SELECT $attr FROM HOSTS WHERE hostName=?", undef, {bind_values => [$hostName]});
}

sub getFieldsFromHostsEx {
  my $self   = shift;
  my $attr   = shift || "*";
  my $addsql = shift || "";

  $DEBUG
    and $self->debug(1, "In getFieldsFromHostsEx fetching attributes $attr with condition $addsql from table HOSTS");
  $self->query("SELECT $attr FROM HOSTS $addsql", undef, @_);
}

sub getFieldFromHostsEx {
  my $self   = shift;
  my $attr   = shift || "*";
  my $addsql = shift || "";

  $DEBUG
    and $self->debug(1, "In getFieldFromHostsEx fetching attributes $attr with condition $addsql from table HOSTS");
  $self->queryColumn("SELECT $attr FROM HOSTS $addsql", undef, @_);
}

###		SITES

sub getSitesByDomain {
  my $self   = shift;
  my $domain = shift
    or $self->{LOGGER}->error("TaskQueue", "In getSitesByDomain domain is missing")
    and return;
  my $attr = shift || "*";

  $DEBUG
    and $self->debug(1, "In getSitesByDomain fetching attributes $attr for domain $domain");
  $self->query("SELECT $attr FROM SITES where domain=?", undef, {bind_values => [$domain]});
}

sub insertSite {
  shift->insert("SITES", @_);
}

sub updateSites {
  shift->update("SITES", @_);
}

sub deleteFromSites {
  shift->delete("SITES", @_);
}

###		MESSAGES

sub insertMessage {
  shift->insert("MESSAGES", @_);
}

sub updateMessages {
  shift->update("MESSAGES", @_);
}

sub deleteFromMessages {
  shift->update("MESSAGES", @_);
}


###             SITEQUEUE
sub setSiteQueueTable {
  my $self = shift;
  $self->{SITEQUEUETABLE} = (shift or "SITEQUEUES");
}

sub resyncSiteQueueTable {
  my $self = shift;
  $self->info("Extracting all sites from the QUEUE ....");
  my $allsites = $self->queryColumn("select distinct site from QUEUE");
  @$allsites
    or $self->info("Warning: at the moment there are no sites defined in your organization")
    and return 1;

  my $site;
  my $now = time;
  my $qstat;

  my $sql=" update SITEQUEUES join (select sum(cost) REALCOST, ";
  my $set=" Group by status) dd) bb set cost=REALCOST, ";

  foreach my $stat (@{AliEn::Util::JobStatus()}) {
  	  $sql.=" max(if(status='$stat', count, 0)) REAL$stat,";
  	  $set.=" $stat=REAL$stat,"      
    }
  $set =~ s/,$/ where site=?/;
  $sql =~ s/,$/ from (select status, sum(cost) as cost, count(*) as count from QUEUE join QUEUEPROC using(queueid) where site/;
  

  foreach my $siteName (@$allsites) {
    my @bind=();
    my $realSiteName=$siteName;
    if ($siteName ){
    	$site="=?";
    	@bind=$siteName;
    }else{
    	$site=" is null ";
    	$realSiteName="UNASSIGNED::SITE";
    }
    push @bind, $realSiteName;
    $self->info("Doing site '$realSiteName'");

    $self->info("$sql $site $set ");
	  $self->do("$sql $site $set", {bind_values=>[@bind]});
  }
  return 1;
}

sub checkSiteQueueTable {
  my $self = shift;
  $self->{SITEQUEUETABLE} = (shift or "SITEQUEUES");

  my %columns = (
    site        => "varchar(40) not null",
    cost        => "float",
    status      => "varchar(25)",
    statustime  => "int(20)",
    blocked     => "varchar(20)",
    maxqueued   => "int",
    maxrunning  => "int",
    queueload   => "float",
    runload     => "float",
    jdl         => "text",
    jdlAgent    => 'text',
    timeblocked => "datetime",
  );

  foreach (@{AliEn::Util::JobStatus()}) {
    $columns{$_} = "int";
  }
  $self->checkTable($self->{SITEQUEUETABLE}, "site", \%columns, "site");
}

sub setSiteQueueStatus {
  my $self   = shift;
  my $site   = shift or return;
  my $status = shift or return;
  my $jdl    = shift || "";
  my $set    = {};
  $set->{site}       = $site;
  $self->info("IN SETSITEQUEUESTATUS with $site\n");
  if ("$site" =~ /HASH/){
    $self->info("THE NAME THAT WE GOT IS AHASHS\n");
    use Data::Dumper;
    $self->info(Dumper($site));
    
  }
  $set->{status}     = "$status";
  $set->{statustime} = time;
  if ($jdl) {
    my $field = "jdl";
    ($status =~ /jobagent-no-match/) and $field = "jdlagent";
    $set->{$field} = $jdl;
  }

  my $done = $self->updateSiteQueue($set, "site=?", {bind_values => [$site]});
  if ($done =~ /^0E0$/) {
    $self->insertSiteQueue($set);
  }
}

sub deleteSiteQueue {
  my $self = shift;
  $self->delete("$self->{SITEQUEUETABLE}", @_);
}

sub updateSiteQueue {
  my $self = shift;
  $self->update("$self->{SITEQUEUETABLE}", @_);
}

sub insertSiteQueue {
  my $self = shift;
  $self->insert("$self->{SITEQUEUETABLE}", @_);
}

sub getFieldFromSiteQueue {
  my $self = shift;
  my $site = shift
    or $self->{LOGGER}->error("TaskQueue", "In getFieldFromSiteQueue site name is missing")
    and return;
  my $attr = shift || "*";

  $DEBUG
    and $self->debug(1, "In getFieldFromSiteQueue fetching attribute $attr of site $site");
  $self->queryValue("SELECT $attr FROM $self->{SITEQUEUETABLE} WHERE site=?", undef, {bind_values => [$site]});
}

sub getFieldsFromSiteQueue {
  my $self = shift;
  my $site = shift
    or $self->{LOGGER}->error("TaskQueue", "In getFieldsFromSiteQueue site name is missing")
    and return;
  my $attr = shift || "*";

  $DEBUG
    and $self->debug(1, "In getFieldsFromSiteQueue fetching attributes $attr of site name");
  $self->queryRow("SELECT $attr FROM $self->{SITEQUEUETABLE} WHERE site=?", undef, {bind_values => [$site]});
}

sub getFieldsFromSiteQueueEx {
  my $self   = shift;
  my $attr   = shift || "*";
  my $addsql = shift || "";

  $DEBUG
    and $self->debug(1,
    "In getFieldsFromSiteQueueEx fetching attributes $attr with condition $addsql from table $self->{SITEQUEUETABLE}");
  $self->query("SELECT $attr FROM $self->{SITEQUEUETABLE} $addsql", undef, @_);
}

sub getFieldFromSiteQueueEx {
  my $self   = shift;
  my $attr   = shift || "*";
  my $addsql = shift || "";

  $DEBUG
    and $self->debug(1,
    "In getFieldFromSiteQueueEx fetching attributes $attr with condition $addsql from table $self->{SITEQUEUETABLE}");
  $self->queryColumn("SELECT $attr FROM $self->{SITEQUEUETABLE} $addsql", undef, @_);
}

###     Priority table
sub setPriorityTable {
  my $self = shift;
  $self->{PRIORITYTABLE} = (shift or "PRIORITY");
}

###     QUEUE Copy
sub insertEntry {
  my $self     = shift;
  my $dsttable = (shift or return);
  my $href     = (shift or return);
  my $newhash  = {};

  my $queue = $self->describeTable("$dsttable");

  defined $queue
    or return;

  foreach (@$queue) {
    if (($_->{Field} ne "")) {
      $newhash->{$_->{Field}} = $href->{$_->{Field}};
    }
  }

  $DEBUG and $self->debug(1, "Copy Entry to $dsttable");
  $self->insert($dsttable, $newhash);
}

#### JobAgent

sub extractFieldsFromReq {
  my $self= shift;
  my $text =shift;
  my $params= {counter=> 1, ttl=>84000, disk=>0, packages=>'%', partition=>'%', ce=>''};

  my $site = "";
  while ($text =~ s/member\(other.CloseSE,"[^:]*::([^:]*)::[^:]*"\)//si) {
    $site =~ /,$1/ or $site .= ",$1";
  }
  $site and $site .= ",";
  $params->{site} = $site;
  
  my @packages;
  while ($text =~ s/member\(other.Packages,"([^"]*)"\)//si ) {
    grep /^$1$/, @packages or 
      push @packages, $1;
  }
  if (@packages) {
    @packages=sort @packages;
    $params->{packages}= '%,' . join (',%,', sort @packages) .',%';
  }
  
  $text =~ s/other.TTL\s*>\s*(\d+)//i and $params->{ttl} = $1;
  $text =~ s/\suser\s*=\s*"([^"]*)"//i and $params->{user} = $1;
  $text =~ s/other.LocalDiskSpace\s*>\s*(\d*)// and $params->{disk}=$1; 
  $text =~ s/other.GridPartitions,"([^"]*)"//i and $params->{partition}=$1; 
  $text =~ s/other.ce\s*==\s*"([^"]*)"//i and $params->{ce}=$1;
  $text =~ s/this.filebroker\s*==\s*1//i and $params->{fileBroker}=1 and $self->info("DOING FILE BROKERING!!!");
 
  $self->info("The ttl is $params->{ttl} and the site is in fact '$site'. Left  '$text' ");
  return $params;
}
sub insertJobAgent {
  my $self = shift;
  my $text = shift;
  
 
  $self->info("Inserting a jobagent with '$text'");
  my $params=$self->extractFieldsFromReq($text);
    $params or
      $self->info("Error getting the fields from '$text'") and return;
  
  my $req="1=1 ";
  my @bind=();

  foreach my $key (keys %$params) {
    $key eq "counter" and next;
    $req .= " and $key = ?"; 
    push @bind, $params->{$key};  	
  }
  $self->info("QUERY: SELECT entryId from JOBAGENT where $req;, and @bind");
  my $id = $self->queryValue("SELECT entryId from JOBAGENT where $req", undef, {bind_values => [@bind]});
  
  if (!$id) {
    use Data::Dumper;
    $self->info("We don't have anything that matches". Dumper($req, @bind));
    if (!$self->insert("JOBAGENT", $params )) {
      $self->info("Error inserting the new jobagent");
      return;
    }
    $id = $self->getLastId("JOBAGENT");
    $self->info("And we have the id JOBAGENT");
  } else {
    $self->do("UPDATE JOBAGENT set counter=counter+1 where entryId=?", {bind_values => [$id]});
  }
  $self->info("Jobagent inserted $id");
  return $id;
}

sub deleteJobAgent {
  my $self = shift;
  my $id   = shift;
  $self->info("Deleting a jobagent for '$id'");
  my $done = $self->do("update JOBAGENT set counter=counter-1 where entryId=?", {bind_values => [$id]});
  $self->delete("JOBAGENT", "counter<1");
  return $done;
}

# send a job's status to MonaLisa
sub sendJobStatus {
  my $self = shift;
  my ($jobID, $newStatus, $execHost, $submitHost) = @_;

  if ($self->{MONITOR}) {
    my $statusID = AliEn::Util::statusForML($newStatus);
    $execHost = $execHost || "NO_SITE";
    my @params = ("jobID", $jobID, "statusID", $statusID);
    push(@params, "submitHost", "$jobID/$submitHost") if $submitHost;
    $self->{MONITOR}->sendParameters("TaskQueue_Jobs_" . $self->{CONFIG}->{ORG_NAME}, $execHost, @params);
  }
}

sub retrieveJobMessages {
  my $self = shift;
  my $time = time;
  my $info = $self->query("SELECT * from JOBMESSAGES where timestamp < ?", undef, {bind_values => [$time]});
  $self->delete("JOBMESSAGES", "timestamp < ?", {bind_values => [$time]});
  return $info;
}

sub insertJobMessage {
  my $self    = shift;
  my $jobId   = shift;
  my $tag     = shift;
  my $message = shift;
  my $time    = time;
  return $self->insert(
    "JOBMESSAGES",
    { jobId     => $jobId,
      procinfo  => $message,
      tag       => $tag,
      timestamp => $time
    }
  );

}

sub getNumberWaitingForSite{
  my  $self=shift;
  my $options=shift;
  my @bind=();
  my $where="";
  my $return= "sum(counter)";
  
  $options->{ttl} and $where.="and ttl < ?  " and push @bind, $options->{ttl};
  $options->{disk} and $where.="and disk < ?  " and push @bind, $options->{disk};
  $options->{site} and $where.="and (site='' or site like concat('%,',?,',%') )" and push @bind, $options->{site};   
  defined $options->{packages} and $where .="and ? like packages " and push @bind, $options->{packages};
  $options->{partition} and $where .="and ? like partition " and push @bind, $options->{partition};
  $options->{ce} and $where.=" and (ce='' or ce=?)" and push @bind,$options->{ce};
  $options->{returnPackages} and $return="packages";
  my $method="queryValue";
  if ($options->{returnId}){
    $return="entryId,fileBroker";
    $method="queryRow";
	}
  

  
  return $self->$method("select $return from JOBAGENT where 1=1 $where  limit 1", undef, {bind_values=>\@bind});
  
}

sub getWaitingJobForAgentId{ 
  my $self=shift;
  my $agentid=shift;
  my $cename=shift || "no_user\@no_site";
  $self->info("Getting a waiting job for $agentid");

  my $done=$self->do("UPDATE QUEUE set status='ASSIGNED',exechost=?,site=?
   where status='WAITING' and agentid=? and \@assigned_job:=queueid  limit 1",
                     {bind_values=>["no_user\@$cename", $cename, $agentid ]});
  
  if ($done>0){
  	my $info=$self->queryRow("select queueid, origjdl jdl,  substring(submithost, 1,locate('\@', submithost)-1 ) user from 
  	QUEUEJDL join QUEUE using (queueid) where queueid=\@assigned_job");
  	$info or $self->info("Error checking what we selected") and return;
  	$self->info("AND NOW ");
  	$self->do("update SITEQUEUES set ASSIGNED=ASSIGNED+1 where site=?",{bind_values=>[$cename]});
  	$self->deleteJobAgent($agentid);
  	$self->info("Giving back the job $info->{queueid}");
  	return ($info->{queueid}, $info->{jdl}, $info->{user});
  }
  $self->info("There were no jobs waiting for agent $agentid");
  $self->do("DELETE FROM JOBAGENT where entryid=?", {bind_values=>[$agentid]}); 
  return;
  	
	
}

sub resubmitJob{
	my $self=shift;
	my $queueid=shift;
	
	$self->do("update QUEUEJDL set resultsJdl=null where queueid=?",{bind_values=>[$queueid]} );
	my $status="WAITING";
	my $data=$self->queryRow("select site,status from QUEUE where queueid=?", undef, {bind_values=>[$queueid]})
	 or $self->info("Error getting the previous status of the job ") and return;
	 
	my $previousStatus=$data->{status};
	my $previousSite= $data->{site} || "UNASSIGNED::SITE";
	$self->info("UPDATING $previousStatus and $previousSite");
	
	
	
	$self->queryValue("select 1 from QUEUE where queueid=? and masterjob=1", undef, {bind_values=>[$queueid]})
	  and $status="INSERTING";
	$self->do('UPDATE QUEUE SET status= ? ,resubmission= resubmission+1 ,started= "" ,finished= "" ,exechost= "",site=NULL,path=NULL  WHERE queueid=? ',
		{bind_values=>[$status, $queueid]	} );
	$self->do("UPDATE SITEQUEUES set $previousStatus=$previousStatus-1 where site=?", {bind_values=>[$previousSite]});
	$self->do("UPDATE SITEQUEUES set WAITING=WAITING+1 where site='UNASSIGNED::SITE'");
  
	#Should we delete the QUEUEPROC??? Nope, that's defined as on cascade delete
	
	#Finally, udpate the JOBAGENT
	
	my $done=$self->do("update JOBAGENT join QUEUE on (agentid=entryid) set counter=counter+1 where queueid=?",
	  {bind_values=>[$queueid]});
	if ($done =~ /0E0/){
		$self->info("The job agent is no longer there!!");
		my $info = $self->queryRow("select origjdl jdl , agentid from QUEUEJDL join QUEUE using (queueid) where queueid=?",
		                          undef, {bind_values=>[$queueid]});
		
		$info or $self->info("Error getting the jdl of the job") and return;
		my $jdl=$info->{jdl};
		$jdl =~ /[\s;](requirements[^;]*).*\]/ims
      or $self->info("Error getting the requirements from $jdl") and return;

    my $req = $1;
    $jdl =~ /(\suser\s*=\s*"([^"]*)")/si or $self->info("Error getting the user from '$jdl'") and next;
    $req.="; $1 ";
    my $params=$self->extractFieldsFromReq($req);
    $params->{entryId}= $info->{agentid};
		$self->insert("JOBAGENT",$params);
		
	}
	
	return $queueid
}

sub insertFileBroker{
	my $self=shift;
	my $masterId=shift;
	my $lfn=shift;
	my @ses=@_;
	
	my $sites=",";
	foreach my $se (@ses){
		$se=~ /::(.*)::/ and $sites.=lc($1) .",";
	}
	
	$self->insert("FILES_BROKER", {split=>$masterId, lfn=>$lfn, sites=>$sites})
}

sub killTask{
	my $self=shift;
	my $queueId = shift;
  my $user    = shift;

  # check for subjob's ....
  my $rresult =
    $self->queryColumn("SELECT queueId from QUEUE where (queueId=? or (split!=0 and split=?)) ",
    undef, {bind_values=>[$queueId, $queueId]});
  my @retvalue;

  for my $j (@$rresult) {
    @retvalue = $self->killProcessInt($j, $user);
  }
  return @retvalue;
}

sub killProcessInt {
  my $self    = shift;
  my $queueId = shift;
  my $user    = shift;

  my $date = time;

  ($queueId)
    or $self->info("In killProcess no queueId in killProcess!!")
    and return;

  $self->info("Killing process $queueId...");

  my ($data) = $self->getFieldsFromQueue($queueId, "status,exechost, submithost, finished,site,agentid");

  defined $data
    or $self->info( "In killProcess error during execution of database query")
    and return;

  %$data
    or $self->info("In killProcess process $queueId does not exist")
    and return;

  #my ( $status, $host, $submithost, $finished ) = split "###", $data;
  $data->{exechost} =~ s/^.*\@//;

  if (($data->{submithost} !~ /^$user\@/) and ($user ne "admin")) {
    $self->info( "In killProcess process does not belong to '$user'");
    return;
  }

  $self->do("delete from QUEUE where queueid=?", {bind_values=>[$queueId]}) or return ;
  my $siteName=( $data->{site} || "UNASSIGNED::SITE");
  $self->do("update SITEQUEUES set $data->{status}=$data->{status}-1 where site=?", {bind_values=>[$siteName]});
  $self->insertJobMessage($queueId, "state", "Job has been killed");
  if ($data->{status} =~ /WAITING/){
  	$self->info("And reducing the number of agents");
  	$self->do("update JOBAGENT set counter=counter-1 where entryid=?", {bind_values=>[$data->{agentid}]})
  	
  }
  if ($data->{exechost}) {
    my ($port) = $self->getFieldFromHosts($data->{exechost}, "hostport")
      or $self->info("Unable to fetch hostport for host $data->{exechost}")
      and return;

    $DEBUG and $self->debug(1, "Sending a signal to $data->{exechost} $port to kill the process... ");
    my $current = time() + 300;
    my ($ok) = $self->insertMessage(
      { TargetHost    => $data->{exechost},
        TargetService => 'ClusterMonitor',
        Message       => 'killProcess',
        MessageArgs   => $queueId,

        #	Expires=>'UNIX_TIMESTAMP(Now())+300'});
        Expires => $current
      }
    );

    ($ok)
      or $self->info( "In killProcess error inserting the message")
      and return;
  }
  $self->info("Process killed");

  return 1;
}

# JOBTOKEN 

sub deleteJobToken {
  my $self = shift;
  my $id   = shift
    or $self->{LOGGER}->error("TaskQueue", "In deleteJobToken job id is missing")
    and return;

  $self->debug(1, "In deleteJobToken deleting token for job $id");
  return $self->delete("JOBTOKEN", "jobId= ?", {bind_values => [$id]});
}

sub getFieldFromJobToken {
  my $self = shift;
  my $id   = shift
    or $self->{LOGGER}->error("TaskQueue", "In getFieldFromJobToken job id is missing")
    and return;
  my $attr = shift || "jobId,userName,jobToken";

  $self->debug(1, "In getFieldFromJobToken fetching attribute $attr for job id $id from table jobToken");
  return $self->queryValue("SELECT $attr FROM JOBTOKEN WHERE jobId= ?", undef, {bind_values => [$id]});
}

sub getFieldsFromJobToken {
  my $self = shift;
  my $id   = shift
    or $self->{LOGGER}->error("TaskQueue", "In getFieldsFromJobToken job id is missing")
    and return;
  my $attr = shift || "jobId,userName,jobToken";

  $self->debug(1, "In getFieldsFromJobToken fetching attributes $attr for job id $id from table jobToken");
  return $self->queryRow("SELECT $attr FROM JOBTOKEN WHERE jobId= ?", undef, {bind_values => [$id]});
}

sub setJobToken {
  my $self = shift;
  my $id   = shift
    or $self->{LOGGER}->error("TaskQueue", "In setJobToken job id is missing")
    and return;
  my $token = shift;

  $self->debug(1, "In setJobToken updating token for job $id");
  return $self->update("JOBTOKEN", {jobToken => $token}, "jobId= ?", {bind_values => [$id]});
}

sub insertJobToken {
  my $self  = shift;
  my $id    = shift;
  my $user  = shift;
  my $token = shift;

  $self->debug(1, "In insertJobToken inserting new data into table JOBTOKEN");
  return $self->insert("JOBTOKEN", {jobId => $id, userName => $user, jobToken => $token});
}

sub getUsername {
  my $self = shift;
  my $id   = shift
    or $self->{LOGGER}->error("TaskQueue", "In getUsername job id is missing")
    and return;
  my $token = shift
    or $self->{LOGGER}->error("TaskQueue", "In getUsername job token is missing")
    and return;
  $token =~ /^-1$/ and $self->{LOGGER}->info("TaskQueue", "The job token is not valid") and return;
  $self->debug(1, "In getUsername fetching user name for job $id and token $token");
  return $self->queryValue("SELECT userName FROM JOBTOKEN where jobId=? and jobToken= ?",
    undef, {bind_values => [ $id, $token ]});
}

sub checkPriorityTable {
  my $self = shift;
  $self->{PRIORITYTABLE} = (shift or "PRIORITY");

  my %columns = (
    user                => "varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs not null",
    priority            => "float default 0 not null ",
    maxparallelJobs     => "int default 0 not null  ",
    nominalparallelJobs => "int default 0 not null ",
    computedpriority    => "float default 0 not null ",
    waiting             => "int default 0 not null ",
    running             => "int default 0 not null ",
    userload            => "float default 0 not null ",

    #Job Quota
    unfinishedJobsLast24h   => "int default 0 not null ",
    totalRunningTimeLast24h => "bigint default 0 not null ",
    totalCpuCostLast24h     => "float default 0 not null ",
    maxUnfinishedJobs       => "int default 0 not null ",
    maxTotalRunningTime     => "bigint default 0 not null ",
    maxTotalCpuCost         => "float default 0  not null ",
    ##File Quota
    #nbFiles=>"int default 0 not null ",
    #totalSize=>"bigint  default 0 not null",
    #maxNbFiles=>"int default 0 not null ",
    #maxTotalSize=>"bigint default 0  not null ",
    #tmpIncreasedNbFiles=>"int default 0 not null ",
    #tmpIncreasedTotalSize=>"bigint  default 0 not null ",
  );

  $self->checkTable($self->{PRIORITYTABLE}, "user", \%columns, $self->reservedWord('user'));

}

sub checkPriorityValue() {
  my $self = shift;
  my $user = shift or $self->{LOGGER}->error("TaskQueue", "no username provided in checkPriorityValue");
  $self->debug(1, "Checking if the user $user exists");

  my $exists = $self->getFieldFromPriority("$user", "count(*)");
  if ($exists) {
    $self->debug(1, "$user entry for priority exists!");
  } else {
    $self->debug(1, "$user entry for priority does not exist!");
    my $set = {};
    $set->{'user'}                = "$user";
    $set->{'priority'}            = "1.0";
    $set->{'maxparallelJobs'}     = 20;
    $set->{'nominalparallelJobs'} = 10;
    $set->{'computedpriority'}    = 1;

    #Job Quota
    $set->{'unfinishedJobsLast24h'}   = 0;
    $set->{'maxUnfinishedJobs'}       = 60;
    $set->{'totalRunningTimeLast24h'} = 0;
    $set->{'maxTotalRunningTime'}     = 1000000;
    $set->{'totalCpuCostLast24h'}     = 0;
    $set->{'maxTotalCpuCost'}         = 2000000;
    ##File Quota
    #$set->{'nbFiles'} = 0;
    #$set->{'totalSize'} = 0;
    #$set->{'tmpIncreasedNbFiles'} = 0;
    #$set->{'tmpIncreasedTotalSize'} = 0;
    #$set->{'maxNbFiles'}=10000;
    #$set->{'maxTotalSize'}=10000000000;
    $self->insertPrioritySet($user, $set);
  }
}

sub insertPriority {
  my $self = shift;
  $self->insert("$self->{PRIORITYTABLE}", @_);
}

sub updatePriority {
  my $self = shift;
  $self->update("$self->{PRIORITYTABLE}", @_);
}

sub insertPrioritySet {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("TaskQueue", "In insertPrioritySet user is missing")
    and return;
  my $set = shift;

  $self->debug(1, "In insertPrioritySet user is missing");
  $self->insert($self->{PRIORITYTABLE}, $set);
}

sub updatePrioritySet {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("TaskQueue", "In updatePrioritySet user is missing")
    and return;
  my $set = shift;

  $self->debug(1, "In updatePrioritySet user is NOT missing");
  $self->update("$self->{PRIORITYTABLE}", $set, $self->reservedWord("user") . " LIKE ?", {bind_values => [$user]});
}

sub getFieldFromPriority {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("TaskQueue", "In getFieldFromPriority user is missing")
    and return;
  my $attr = shift || "*";

  $self->debug(1, "In getFieldFromPriority fetching attribute $attr of user $user");
  $self->queryValue("SELECT $attr FROM $self->{PRIORITYTABLE} WHERE user =?", undef, {bind_values => [$user]});
}

sub getFieldsFromPriority {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("TaskQueue", "In getFieldsFromPriority user is missing")
    and return;
  my $attr = shift || "*";

  $self->debug(1, "In getFieldsFromPriority fetching attributes $attr of user $user");
  $self->queryRow("SELECT $attr FROM $self->{PRIORITYTABLE} WHERE user=?", undef, {bind_values => [$user]});
}

sub getFieldsFromPriorityEx {
  my $self   = shift;
  my $attr   = shift || "*";
  my $addsql = shift || "";

  $self->debug(1,
    "In getFieldsFromPriorityEx fetching attributes $attr with condition $addsql from table $self->{PRIORITYTABLE}");
  $self->query("SELECT $attr FROM $self->{PRIORITYTABLE} $addsql", undef, @_);
}


# checkJobQuota, migrated from Job (Manager)
sub checkJobQuota {
  my $self = shift;
  my $user = shift
    or $self->info("In checkJobQuota user is missing\n")
    and return (-1, "user is missing");
  my $nbJobsToSubmit = shift;
  (defined $nbJobsToSubmit)
    or $self->info("In checkJobQuota nbJobsToSubmit is missing\n")
    and return (-1, "nbJobsToSubmit is missing");

  $DEBUG and $self->debug(1, "In checkJobQuota user:$user, nbJobs:$nbJobsToSubmit");

  my $array = $self->getFieldsFromPriorityEx(
"unfinishedJobsLast24h, maxUnfinishedJobs, totalRunningTimeLast24h, maxTotalRunningTime, totalCpuCostLast24h, maxTotalCpuCost",
    "where " . $self->reservedWord("user") . " like '$user'"
    )
    or $self->info("Failed to getting data from PRIORITY table")
    and return (-1, "Failed to getting data from PRIORITY table");
  $array->[0]
    or $self->{LOGGER}->error("User $user not exist")
    and return (-1, "User $user not exist in PRIORITY table");

  my $unfinishedJobsLast24h   = $array->[0]->{'unfinishedJobsLast24h'};
  my $maxUnfinishedJobs       = $array->[0]->{'maxUnfinishedJobs'};
  my $totalRunningTimeLast24h = $array->[0]->{'totalRunningTimeLast24h'};
  my $maxTotalRunningTime     = $array->[0]->{'maxTotalRunningTime'};
  my $totalCpuCostLast24h     = $array->[0]->{'totalCpuCostLast24h'};
  my $maxTotalCpuCost         = $array->[0]->{'maxTotalCpuCost'};

  $DEBUG and $self->debug(1, "nbJobs: $nbJobsToSubmit, unfinishedJobs: $unfinishedJobsLast24h/$maxUnfinishedJobs");
  $DEBUG and $self->debug(1, "totalRunningTime: $totalRunningTimeLast24h/$maxTotalRunningTime");
  $DEBUG and $self->debug(1, "totalCpuCostLast24h: $totalCpuCostLast24h/$maxTotalCpuCost");

  if ($nbJobsToSubmit + $unfinishedJobsLast24h > $maxUnfinishedJobs) {
    $self->info("In checkJobQuota $user: Not allowed for nbJobs overflow");
    return (-1,
"DENIED: You're trying to submit $nbJobsToSubmit jobs. That exceeds your limit (at the moment,  $unfinishedJobsLast24h/$maxUnfinishedJobs)."
    );
  }

  if ($totalRunningTimeLast24h >= $maxTotalRunningTime) {
    $self->info("In checkJobQuota $user: Not allowed for totalRunningTime overflow");
    return (-1, "DENIED: You've already executed your jobs for enough time.");
  }

  if ($totalCpuCostLast24h >= $maxTotalCpuCost) {
    $self->info("In checkJobQuota $user: Not allowed for totalCpuCost overflow");
    return (-1, "DENIED: You've already used enough CPU.");
  }

  $self->info("In checkJobQuota $user: Allowed");
  return (1, undef);
}

=head1 NAME

AliEn::Database::TaskQueue

=head1 DESCRIPTION

The AliEn::Database::TaskQueue module extends AliEn::Database module. Module
contains method specific for tables from database processes.

=head1 SYNOPSIS


=item C<deleteJob>     

  $res = $dbh->deleteJob($id);
  
Method deletes job with id $id.
If job id is not defined method will return undef and report error.

=back

=head1 SEE ALSO

AliEn::Database

=cut

1;

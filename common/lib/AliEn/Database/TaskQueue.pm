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
    'UPDATING'   => 100,

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
      userId       => "int ",
      execHostId   => "int",
      submitHostId => "int",
      priority     => "tinyint(4)",
      statusId     => "tinyint not null",
      received     => "int(20)",
      started      => "int(20)",
      finished     => "int(20)",
      expires      => "int(10)",
      error        => "int(11)",
      validate     => "int(1)",
      sent         => "int(20)",
      siteId       => "int(20) not null",
      nodeId       => "int",
      split        => "int",
      splitting    => "int",
      merging      => "varchar(64)",
      masterjob    => "int(1) default 0",
      price        => "float",
      chargeStatus => "varchar(20)",
      optimized    => "int(1) default 0",
      finalPrice   => "float",
      notifyId     => "int",
      agentId      => "int(11)",
      mtime        => "timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
      resubmission => "int(11) not null default 0",
      commandId    => "int(11)",
    },
    id          => "queueId",
    index       => "queueId",
    extra_index => [
      "INDEX (split)",
      "foreign key (statusId) references QUEUE_STATUS(statusId) on delete cascade",
      "foreign key (notifyId) references QUEUE_NOTIFY(notifyId) on delete cascade",
      "foreign key (userId) references QUEUE_USER(userId) on delete cascade",
      "foreign key (siteId) references SITEQUEUES(siteId) on delete cascade",
      "foreign key (exechostId) references QUEUE_HOST(hostId) on delete cascade",
      "foreign key (submithostId) references QUEUE_HOST(hostId) on delete cascade",
      "foreign key (nodeId) references QUEUE_HOST(hostId) on delete cascade",
      "foreign key (commandId) references QUEUE_COMMAND(commandId) on delete cascade",
      "foreign key (agentId) references JOBAGENT(entryId) on delete set null",
      "INDEX(agentId)",      
      "INDEX(priority)",
      "INDEX (siteId,statusId)",
      "INDEX (sent)",
      "INDEX (statusId,agentId)",
      "UNIQUE INDEX (statusId,queueId)"
    ],
    order=>13
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
      spyurl       => "varchar(64)",
    },
    id    => "queueId",
    extra_index=> ['foreign key (queueId) references QUEUE(queueId) on delete cascade'],
    order=>14
  };
  my $queueColumnsArchive = {
    columns => {
      #QUEUE
      queueId      => "int(11) not null auto_increment primary key",
      userId       => "int ",
      execHostId   => "int",
      submitHostId => "int",
      priority     => "tinyint(4)",
      statusId     => "tinyint not null",
      received     => "int(20)",
      started      => "int(20)",
      finished     => "int(20)",
      expires      => "int(10)",
      error        => "int(11)",
      validate     => "int(1)",
      sent         => "int(20)",
      siteId       => "int(20) not null",
      nodeId       => "int",
      split        => "int",
      splitting    => "int",
      merging      => "varchar(64)",
      masterjob    => "int(1) default 0",
      price        => "float",
      chargeStatus => "varchar(20)",
      optimized    => "int(1) default 0",
      finalPrice   => "float",
      notifyId     => "int",
      mtime        => "timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
      resubmission => "int(11) not null default 0",
      commandId    => "int(11)",
      #QUEUEPROC
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
      lastupdate   => "datetime ",
      batchid      => "varchar(255)",
      spyurl       => "varchar(64)",
      #QUEUEJDL
      origJdl      => "varbinary(30000)",
      resultsJdl   => "varbinary(10000)",
      path         => "varchar(255)",
    },
    id          => "queueId",
    index       => "queueId",
    extra_index => [
      "INDEX (split)",
      "foreign key (statusId) references QUEUE_STATUS(statusId) on delete cascade",
      "foreign key (notifyId) references QUEUE_NOTIFY(notifyId) on delete cascade",
      "foreign key (userId) references QUEUE_USER(userId) on delete cascade",
      "foreign key (siteId) references SITEQUEUES(siteId) on delete cascade",
      "foreign key (exechostId) references QUEUE_HOST(hostId) on delete cascade",
      "foreign key (submithostId) references QUEUE_HOST(hostId) on delete cascade",
      "foreign key (nodeId) references QUEUE_HOST(hostId) on delete cascade",
      "foreign key (commandId) references QUEUE_COMMAND(commandId) on delete cascade",
      "INDEX(priority)",
      "INDEX (siteId,statusId)",
      "INDEX (sent)",
      "UNIQUE INDEX (statusId,queueId)"
    ],
    order=>15
  };
  my $queueColumnsJDL ={
  	columns=>{    
  		queueId      => "int(11) not null",
  		origJdl      => "varbinary(30000)",
  		resultsJdl   => "varbinary(10000)",
        path         => "varchar(255)",
  		
  	},
  	id =>"queueId",
  	extra_index=> ['foreign key (queueId) references QUEUE(queueId) on delete cascade'],
    order=>16
  };
  my $queueColumnsJobtoken ={
  	columns=>{    
		"jobId"    => "int(11)  DEFAULT '0' NOT NULL",
		"userName" => "char(20) DEFAULT NULL",
		"jobToken" => "char(255) DEFAULT NULL",	
  	},
  	id =>"jobId",
  	extra_index=> ['foreign key (jobId) references QUEUE(queueId) on delete cascade'],
    order=>17
  };
  
  my %tables = (
  	QUEUE_STATUS => {
      columns => {
        statusId  => "tinyint not null primary key",
        status    => "varchar(12) not null unique",
      },
      id     => "statusId",
      index  => "statusId",
      order=>1
    },
    QUEUE_NOTIFY => {
      columns => {
        notifyId  => "int not null auto_increment primary key",
        notify    => "varchar(255) not null unique",
      },
      id          => "notifyId",
      index       => "notifyId",
      order=>2
    },
    QUEUE_HOST => {
      columns => {
        hostId  => "int not null auto_increment primary key",
        host    => "varchar(255) not null unique",
      },
      id          => "hostId",
      index       => "hostId",
      order=>3
    },
    QUEUE_COMMAND => {
      columns => {
        commandId  => "int not null auto_increment primary key",
        command    => "varchar(255) not null unique",
      },
      id          => "commandId",
      index       => "commandId",
      order=>4
    },
    QUEUE_USER => {
      columns => {
        userId  => "int not null auto_increment primary key",
        user    => "varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs not null unique"
      },
      id          => "userId",
      index       => "userId",
      order=>5
    },
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
        noce         => "varchar(50) COLLATE latin1_general_ci",
        userId       => "int not null",
        fileBroker   => "tinyint(1) default 0 not null",
      },
      id          => "entryId",
      index       => "entryId",
      extra_index => [ "INDEX(priority)", "INDEX(ttl)", "foreign key (userId) references QUEUE_USER(userId) on delete cascade" ],
      order=>6
    },
    
    QUEUE            => $queueColumns,
    QUEUEPROC        => $queueColumnsProc,
    $self->{QUEUEARCHIVE} => $queueColumnsArchive,
    QUEUEJDL         => $queueColumnsJDL,
    JOBTOKEN    => $queueColumnsJobtoken,
    
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
      order=>7
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
      index => "hostId",
      order=>8
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
      order=>9
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
      order=>10
    },

    JOBSTOMERGE => {
      columns => {masterId => "int(11) not null primary key"},
      id      => "masterId",
      extra_index=>["index(masterId)", "foreign key (masterId) references QUEUE(queueId) on delete cascade" ],
      order=>18
    },
    STAGING => {
      columns => {
        queueId      => "int(11) not null ",
        staging_time => "timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
      },
      id => "queueId",
      extra_index=>[ "foreign key (queueId) references QUEUE(queueId) on delete cascade" ],
      order=>18 
    },
    FILES_BROKER=> {
    	columns=>{"lfn"=> "varchar(255) not null",
    			"split" =>"int(11) not null",
    			"sites"=>"varchar(255) not null",
    			"queueId" => "int(11) default null",
    	},
		id=>"lfn",
    	extra_index=>['index(split)', "unique index(split,lfn)", 
    	              "foreign key (queueId) references QUEUE(queueId) on delete set null",
    	              "foreign key (split) references QUEUE(queueId) on delete cascade"],
        order=>19
    },
    PRIORITY => { 
      columns=>{
	    userId              => "int not null ",
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
     }, 
     id=>"userId", 
     extra_index=>[ "foreign key (userId) references QUEUE_USER(userId) on delete cascade" ], 
     order=>12
    }
 
  );
  
  $self->checkSiteQueueTable("SITEQUEUES")
    or $self->{LOGGER}->error("TaskQueue", "In initialize altering tables failed for SITEQUEUES")
    and return;
  
  foreach my $table (sort {$tables{$a}->{order} <=> $tables{$b}->{order} } keys %tables) {
    $self->checkTable(
      $table,
      $tables{$table}->{id},
      $tables{$table}->{columns},
      $tables{$table}->{index},
      $tables{$table}->{extra_index}
      )
      or $self->{LOGGER}->error("TaskQueue", "Error checking the table $table")
      and return;
  }
  $self->checkJobStatus() or return;


  $self->checkActionTable() or return;
  
  return 1;
}

sub checkJobStatus{
  my $self=shift;
  
  my @values;
  for my $status  ( @{AliEn::Util::JobStatus()}){
    my $id=AliEn::Util::statusForML($status); 
    $self->debug(1, "Inserting $status and $id");
    push @values, "('$status', $id)";
    
  }
  
  $self->do("insert ignore into QUEUE_STATUS (status,statusId) values ". join (",", @values));
  
}

sub setArchive {
  my $self = shift;
  my ($Second, $Minute, $Hour, $Day, $Month, $Year, $WeekDay, $DayOfYear, $IsDST) = localtime(time);
  $Year                     = $Year + 1900;
  $self->{QUEUEARCHIVE}     = "QUEUEARCHIVE" . $Year;
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
  $self->do("INSERT ignore INTO ACTIONS(action)  values ('INSERTING'),  ('MERGING'), 
             ('KILLED'),('SPLITTING'),('STAGING'),('SAVED'),('SAVED_WARN')"
    ) or return;

}

sub insertNotifyEmail{
  my $self=shift;
  my $email=shift;
	
  my $id=$self->queryValue("select notifyId from QUEUE_NOTIFY where notify=?",
	undef, {bind_values=>[$email]});
  $id and return $id;
  $self->info("Inserting the email address of '$email'");
  $self->insert('QUEUE_NOTIFY', {notify=>$email});
  return $self->queryValue("select notifyId from QUEUE_NOTIFY where notify=?",
	undef, {bind_values=>[$email]})
	
}

#sub insertValuesIntoQueue {
sub insertJobLocked {
  my $self = shift;
  my $set  = shift
    or $self->info("Error getting the job to insert")
    and return;
  my $oldjob = (shift or 0);

  $set->{received} = time;

  my ($tmpPrice) = $set->{jdl} =~ /.*price\s*=\s*(\d+.*)\;\s*/i;
  $tmpPrice = sprintf("%.3f", $tmpPrice);
  $set->{price} = $tmpPrice;

  $set->{chargeStatus} = 0;
  
  $set->{commandId}=$self->getOrInsertFromLookupTable('command',
                               $set->{jdl} =~ /.*executable\s*=\s*\"([^\"]*)\"/i);
  $set->{commandId} or $self->info("Error getting the name of the executable from the JDL")
     and return;
  

  #currently $set->{priority} is hardcoded to be '0'

  $DEBUG and $self->debug(1, "In insertJobLocked table $self->{QUEUETABLE} locked. Inserting new data.");
  $set->{jdl} =~ s/'/\\'/g;
  my $jdl=$set->{jdl};
  delete $set->{jdl};
  my $status = $set->{statusId};
  $set->{statusId} = AliEn::Util::statusForML($status);
  $set->{siteid} = $self->queryValue("select siteid from SITEQUEUES where site='unassigned::site'");
  if ($set->{notify}){
    my $notifyId=$self->getOrInsertFromLookupTable('notify', $set->{notify});
                               
    $notifyId or $self->info("Error creating the notification for '$set->{notify}'")
     and return;
	  	
    delete $set->{notify};
    $set->{notifyId}=$notifyId;
  } 
  foreach my $fieldName ('submitHost', 'execHost', 'node') {
    $set->{$fieldName} or next;
    $self->info("Translating $set->{$fieldName}");
    $set->{"${fieldName}Id"}=$self->getOrInsertFromLookupTable('host',$set->{$fieldName});
                               
    $set->{"${fieldName}Id"} or $self->info("Error translating the host $set->{$fieldName} (from $fieldName)")
     and return;
    delete $set->{$fieldName}
  }
  
  my $out = $self->insert("$self->{QUEUETABLE}", $set);

  my $procid = "";
  ($out)
    and $procid = $self->getLastId($self->{QUEUETABLE});

  if ($procid) {
    $DEBUG and $self->debug(1, "In insertJobLocked got job id $procid.");
    $self->insert("QUEUEPROC", {queueId => $procid});
    
    $self->insert("QUEUEJDL", {queueId =>$procid, origJdl=>$jdl},{functions=>{origjdl=>"compress",resultsjdl=>"compress"}});
    my $unassignedId=$self->findSiteId("unassigned::site");
    
    $self->do("update SITEQUEUES set $status=$status+1 where siteid=$unassignedId");
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
  $jdl =~ /split =/im and $action = "SPLITTING";
  ($status !~ /WAITING/)
    and $self->update("ACTIONS", {todo => 1}, "action='$action'");

  # send the new job's status to ML
  $self->sendJobStatus($procid, $status, "", $set->{submitHost});

  return $procid;
}
sub getOrInsertFromLookupTable{
  my $self=shift;
  my $key=shift;
  my $value=shift;
  
  my $table="QUEUE_".uc($key);
  my $id="${key}id";
  
  
  my $hostId=$self->queryValue("select $id from $table where $key=?", undef, {bind_values=>[$value]});
  $hostId and return $hostId;
  $self->info("This is the first time that we see the $key '$value'");
  $self->do("insert into $table ($key) values (?)", {bind_values=>[$value]});
 
  return $self->queryValue("select $id from $table where $key=?", undef, {bind_values=>[$value]});
}

sub getHostName{
  my $self=shift;
  my $hostName=shift;
  my $hostId=$self->queryValue("select hostId from ALL_HOSTS where host=?", undef, {bind_values=>[$hostName]});
  $hostId and return $hostId;
  $self->info("This is the first time that we see the host '$hostName'");
  $self->do("insert into ALL_HOSTS (host) values (?)", {bind_values=>[$hostName]});
 
  return $self->queryValue("select hostId from ALL_HOSTS where host=?", undef, {bind_values=>[$hostName]});
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
sub findSiteId{
  my $self=shift;
  my $site=shift;
  my $siteid = AliEn::Util::returnCacheValue($self,"siteid_$site");
  
  if (! $siteid){
    $siteid=$self->queryValue("SELECT siteid from SITEQUEUES where site=?",
                   undef, {bind_values=>[$site]});
   	AliEn::Util::setCacheValue($self, "siteid_$site", $siteid);
   
  }
  return $siteid; 
 
}


sub updateJob {
  my $self = shift;
  my $id   = shift
    or $self->info( "In updateJob job id is missing")
    and return;
  my $set = shift;
  my $opt = shift || {};
  $self->info("UPDATING THE JOB");
  $set->{statusId} and $set->{statusId} = AliEn::Util::statusForML($set->{statusId});
  
  if ($set->{site}){
   $self->info("READY TO find the siteid of $set->{site}");
   $set->{siteid}=$self->findSiteId($set->{site});
   delete $set->{site};
   $self->info("It is $set->{siteid}");
  }

  if ($set->{node}){
    $set->{nodeid}=$self->getOrInsertFromLookupTable('host',$set->{node});
    delete $set->{node};
  }
  
  
  $DEBUG and $self->debug(1, "In updateJob updating job $id");
  my $procSet = {};
  my $jdlSet  = {};
  foreach my $key (keys %$set) {
    if ($key =~
/(si2k)|(spyurl)|(cpuspeed)|(maxrsize)|(cputime)|(ncpu)|(cost)|(cpufamily)|(cpu)|(vsize)|(rsize)|(runtimes)|(procinfotime)|(maxvsize)|(runtime)|(mem)|(batchid)/
      ) {
      $procSet->{$key} = $set->{$key};
      delete $set->{$key};
    }elsif ($key =~ /((\S*)jdl)|(path)/){
    	$jdlSet->{$key}=$set->{$key};
    	delete $set->{$key};
    }
  }
  my $where = "queueId=?";
  $opt->{where} and $where .= " and $opt->{where}";
  my @bind = ($id);
  $opt->{bind_values} and push @bind, @{$opt->{bind_values}};
  if (keys %$set ) {
    my $done = $self->update($self->{QUEUETABLE}, $set, $where, {bind_values => \@bind});
    #the update didn't work
    $done or return;
    #  the update didn't modify any entries
    $done =~ /^0E0$/ and return;
  }

  if (keys %$procSet) {
    $self->update("QUEUEPROC", $procSet, "queueId=?", {bind_values => [$id]})
      or return;
  }
  if (keys %$jdlSet) {
  	$self->update("QUEUEJDL", $jdlSet, "queueId=?", {bind_values=>[$id], functions=>{origjdl=>"compress",resultsjdl=>"compress"}}) or return;
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
  
  $set->{statusId} and $set->{statusId} = AliEn::Util::statusForML($set->{statusId});  
    
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

  $set->{statusId}       = $status;
  $set->{procinfotime} = time;

  my $message = "";

  $DEBUG
    and $self->debug(1, "In updateStatus checking if job $id with status $oldstatus exists");

  my $oldjobinfo =
    $self->query("SELECT masterjob,siteId,host exechost,statusId,agentid from QUEUE left  join QUEUE_HOST on (exechostid=hostid) where queueid=?",
                  undef, {bind_values => [$id]});

  #Let's take the first entry
  $oldjobinfo and $oldjobinfo = shift @$oldjobinfo;
  if (!$oldjobinfo) {
    $self->{LOGGER}->set_error_msg("The job $id was no longer in the queue");
    $self->info("There was an error: The job $id was no longer in the queue", 1);
    return;
  }
  my $dbsite = $oldjobinfo->{siteId};
  if ($set->{site}){
   $dbsite=$self->findSiteId($set->{site}); 
  }
  
  my $execHost = $set->{execHostId} || $oldjobinfo->{exechost};
  my $dboldstatus = AliEn::Util::statusName($oldjobinfo->{statusId});
  my $masterjob   = $oldjobinfo->{masterjob};
  my $where       = "statusId = ?";

  $self->info("Moving from $dboldstatus to $status ");
  
  if ( ($self->{JOBLEVEL}->{$status} <= $self->{JOBLEVEL}->{$dboldstatus} )
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
  $self->info("Let's do the update");
  #update the value, it is correct
  if (!$self->updateJob($id, $set, {where => "statusId=?", bind_values => [$oldjobinfo->{statusId}]},)) {
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
  $status =~ /^(DONE.*)|(ERROR_.*)|(EXPIRED)|(FAILED)$/
    and $self->checkFinalAction($id, $service);
  $self->info("AND NOW THE STATISTICS for $dbsite modified");
  if ($status ne $oldstatus) {
    if ($status eq "ASSIGNED") {
      $self->info("In updateStatus increasing $status for $dbsite");
      $self->_do("UPDATE $self->{SITEQUEUETABLE} SET $status=$status+1 where siteid=?", {bind_values => [$dbsite]})
        or $message = "TaskQueue: in update Site Queue failed";
    } else {
      $self->info("In updateStatus decreasing $dboldstatus and increasing $status for $dbsite");
      if (
        !$self->_do(
          "UPDATE $self->{SITEQUEUETABLE} SET $dboldstatus = $dboldstatus-1, $status=$status+1 where siteid=?",
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
    $self->queryRow("SELECT statusId,notifyId,split FROM QUEUE where queueid=?", undef, {bind_values => [$id]})
    or return;
  $self->info("Checking if we have to send an email for job $id...");
  $info->{notifyId}
    and $self->sendEmail($info->{notifyId}, $id, $info->{statusId}, $service);
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
  my $notifyId    = shift;
  my $id         = shift;
  my $status     = shift;
  my $service    = shift;

  
  $status = AliEn::Util::statusName($status);
      
  my $address = $self->queryValue("select notify from QUEUE_NOTIFY where notifyid=?", 
                                  undef, {bind_values=>[$notifyId]});
                                  
  $address or $self->info("Error getting the email address of $notifyId") and return;
  
  
  $self->info("We are supposed to send an email ($address)!!! (status $status)");

  my $ua = new LWP::UserAgent;

  $ua->agent("AgentName/0.1 " . $ua->agent);

  my $procdir =  "~/alien-job-$id";

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
  my $ret = $self->queryValue("SELECT $attr FROM $self->{QUEUETABLE} WHERE queueId=?", undef, {bind_values => [$id]});
  
  $attr =~ /statusId/ and $ret = AliEn::Util::statusName($ret);
  return $ret;
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
  my $ret=$self->queryRow("SELECT $attr FROM $self->{QUEUETABLE} $join WHERE queueId=?", undef, {bind_values => [$id]});
  
  $ret->{statusId} and $ret->{statusId} = AliEn::Util::statusName($ret->{statusId});
  return $ret;
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
  my $returnparts = $self->query("SELECT $attr FROM $self->{QUEUETABLE} $addsql", undef, @_);
  
  for (@$returnparts) {
    $_->{statusId} or next;
    $_->{status} =  AliEn::Util::statusName($_->{statusId});
  }
  return $returnparts;
}

sub getFieldFromQueueEx {
  my $self   = shift;
  my $attr   = shift || "*";
  my $addsql = shift || "";

  $DEBUG
    and $self->debug(1,
    "In getFieldFromQueueEx fetching attributes $attr with condition $addsql from table $self->{QUEUETABLE}");
  my $ret = $self->queryColumn("SELECT $attr FROM $self->{QUEUETABLE} $addsql", undef, @_);
  
  $attr =~ /statusId/ and $ret = AliEn::Util::statusName($ret);
  return $ret;
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
  my $query = "SELECT queueid from QUEUE join QUEUEJDL using (queueid) where statusId='$status' $order";
  $query = $self->paginate($query, $limit, 0);

  $DEBUG
    and $self->debug(1, "In getJobsByStatus fetching jobs with statusId $status");
  
  my ($jobs) = $self->query($query, undef, {bind_values => $bind});
  
  foreach my $data (@$jobs){
  	$data->{jdl} = ($self->getJDL($data->{queueid}))->{jdl};
  }
  
  return $jobs;
}

sub getJDL {
  my $self=shift;
  my $id=shift;
 
  $id or return;
 
  my $columns="uncompress(resultsJdl) as resultsJdl, uncompress(origJdl) as origJdl";
  my $join="";
  foreach my $o (@_) {
    $o=~ /-dir/ and $columns.=",path";
    $o=~ /-status/ and $columns.=",statusId" and $join="join QUEUE using (queueid)";
  }

  my $rc=$self->queryRow("select $columns from QUEUEJDL $join where queueId=?", undef, {bind_values=>[$id]});
  $rc->{origJdl} or return;
  $rc->{statusId} and $rc->{status} = AliEn::Util::statusName($rc->{statusId});
  $rc->{resultsJdl} and $rc->{jdl} = "\n".$rc->{origJdl}."\n".$rc->{resultsJdl}
   or $rc->{jdl}="\n".$rc->{origJdl};
  delete $rc->{origJdl};
  delete $rc->{resultsJdl}; 

  return $rc;
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
  my $site = shift || "";
  
  my $allsites;
  if (! $site){
    $self->info("Extracting all sites from the QUEUE ....");
 		$allsites = $self->queryColumn("select site from SITEQUEUES");
    @$allsites
      or $self->info("Warning: at the moment there are no sites defined in your organization")
      and return 1;
  } else{
  	$allsites=[$site];
  }
  my $now = time;
  my $qstat;

  my $sql=" update SITEQUEUES left join (select siteid, sum(cost) REALCOST, ";
  my $set=" Group by statusId, siteid) dd group by siteid) bb using (siteid) set cost=REALCOST, ";

  foreach my $stat (@{AliEn::Util::JobStatus()}) {
  	  $sql.=" max(if(statusId=".AliEn::Util::statusForML($stat).", count, 0)) REAL$stat,";
#  	  $sql.=" max(if(status='$stat', count, 0)) REAL$stat,";
  	  $set.=" $stat=REAL$stat,"      
    }
  $set =~ s/,$//;
  $sql =~ s/,$/ from (select siteid, statusId, sum(cost) as cost, count(*) as count from QUEUE join QUEUEPROC using(queueid)/;
  

 # foreach my $siteName (@$allsites) {
 #   my @bind=();
 #   my $realSiteName=$siteName;
 #   if ($siteName ){
 ##   	$site="=?";
 ##   	@bind=$siteName;
 #   }else{
 #   	$site=" is null ";
 #   	$realSiteName="UNASSIGNED::SITE";
 #   }
 #   push @bind, $realSiteName;
 #   $self->info("Doing site '$realSiteName'");

    $self->info("$sql $set" );
	  $self->do("$sql $set");#, {bind_values=>[@bind]});
#  }
  return 1;
}

sub checkSiteQueueTable {
  my $self = shift;
  $self->{SITEQUEUETABLE} = (shift or "SITEQUEUES");

  my %columns = (
    siteId      => "int(20) not null auto_increment primary key",
    site        => "varchar(40) collate latin1_general_ci not null unique",
    cost        => "float",
    status      => "varchar(25) not null default 'new'",
    statustime  => "int(20) not null default 0",
    blocked     => "varchar(20) not null default 'locked'",
    maxqueued   => "int not null default 0",
    maxrunning  => "int not null default 0",
    queueload   => "float not null default 0",
    runload     => "float",
    jdl         => "text",
    jdlAgent    => 'text',
    timeblocked => "datetime",
  );

  foreach (@{AliEn::Util::JobStatus()}) {
    $columns{$_} = "int not null default 0";
  }
  $self->checkTable($self->{SITEQUEUETABLE}, "siteId", \%columns, "siteId") or return;
  $self->do("insert ignore into SITEQUEUES (site) values ('unassigned::site') ");
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
    $self->insertSiteQueue($site);
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
  my $site=shift;
  
  $self->do("insert into SITEQUEUES (siteid, site) 
            select ifnull(max(siteid)+1,1), ? from SITEQUEUES", 
            {bind_values=>[$site]});
  
  $self->resyncSiteQueueTable($site);
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
sub findUserId{
  my $self=shift;
  my $user=shift;
  
  my $userid = AliEn::Util::returnCacheValue($self,"userid_$user");
  
  if (! $userid){
    $userid=$self->queryValue("SELECT userid from QUEUE_USER where user=?",
                   undef, {bind_values=>[$user]});
   	AliEn::Util::setCacheValue($self, "userid_$user", $userid);
   
  }
  return $userid; 
 
}
sub extractFieldsFromReq {
  my $self= shift;
  my $text =shift;
  my $params= {counter=> 1, ttl=>84000, disk=>0, packages=>'%', partition=>'%', ce=>'', noce=>''};

  my $site = "";
  my $no_se={};
  while ($text =~ s/!member\(\s*other.CloseSE,\s*"([^:]*::[^:]*::[^:]*)"\)//si) {
   $no_se->{uc($1)}=1;
  }
  while ($text =~ s/member\(\s*other.CloseSE,\s*"([^:]*::([^:]*)::[^:]*)"\)//si) {
    $no_se->{uc($1)} and $self->info("Ignoring the SE $1") and next;
    $site =~ /,$2/ or $site .= ",$2";
  }
  $site and $site .= ",";
  $params->{site} = $site;
  
  
  my $noce = "";
  while ($text =~ s/!other.ce\s*==\s*"([^"]*)"//i) {
    $noce =~ /,$1/ or $noce .= ",$1";
  }
  $noce and $noce .= ",";
  $params->{noce} = $noce;
  
  
  my $ce = "";
  while ($text =~ s/other.ce\s*==\s*"([^"]*)"//i) {
    $ce =~ /,$1/ or $ce .= ",$1";
  }
  $ce and $ce .= ",";
  $params->{ce} = $ce;
  
  my @packages;
  while ($text =~ s/member\(\s*other.Packages,\s*"([^"]*)"\)//si ) {
    grep /^$1$/, @packages or 
      push @packages, $1;
  }
  if (@packages) {
    @packages=sort @packages;
    $params->{packages}= '%,' . join (',%,', sort @packages) .',%';
  }
  
  $text =~ s/other.TTL\s*>\s*(\d+)//i and $params->{ttl} = $1;
  if ($text =~ s/\s*user\s*=\s*"([^"]*)"//i){
     $params->{userid}=$self->findUserId($1);   
  } 
  $text =~ s/other.LocalDiskSpace\s*>\s*(\d+)//i and $params->{disk}=$1; 
  $text =~ s/other.GridPartitions,\s*"([^"]*)"//i and $params->{partition}=$1; 
  $text =~ s/this.filebroker\s*==\s*1//i and $params->{fileBroker}=1 and $self->info("DOING FILE BROKERING!!!");
  

  #$self->info("The ttl is $params->{ttl} and the site is in fact '$site'. Left  '$text' ");
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
  defined $options->{installedpackages} and $where .="and ? like packages " and push @bind, $options->{installedpackages};
  $options->{partition} and $where .="and ? like concat('%,',partition, '%,') " and push @bind, $options->{partition};
  $options->{ce} and $where.=" and (ce like '' or ce like concat('%,',?,',%'))" and push @bind,$options->{ce};
  $options->{ce} and $where.=" and noce not like concat('%,',?,',%')" and push @bind,$options->{ce};
  $options->{returnPackages} and $return="packages";
  my $method="queryValue";
  if ($options->{returnId}){
    $return="entryId,fileBroker";
    $method="queryRow";
  }
    
  return $self->$method("select $return from JOBAGENT where 1=1 $where order by priority desc limit 1", undef, {bind_values=>\@bind});
}

sub getWaitingJobForAgentId{ 
  my $self=shift;
  my $agentid=shift;
  my $cename=shift || "no_user\@no_site";
  my $host =shift || "";
  $self->info("Getting a waiting job for $agentid");
 
  
  my $siteid=$self->queryValue("select siteid from SITEQUEUES where site=?",
                               undef, {bind_values=>[$cename]});
                               
  my $hostId=$self->getOrInsertFromLookupTable('host',$host);

  my $done=$self->do("UPDATE QUEUE set statusId=".AliEn::Util::statusForML('ASSIGNED').",siteid=?, exechostid=?
    where statusId=".AliEn::Util::statusForML('WAITING')." and agentid=? and \@assigned_job:=queueid  limit 1", 
                     {bind_values=>[$siteid, $hostId, $agentid ]});
  
  if ($done>0){
  	my $info=$self->queryRow("select queueid, uncompress(origjdl) jdl,  user from 
  	QUEUEJDL join QUEUE using (queueid) join QUEUE_USER using (userid) where queueid=\@assigned_job");
  	$info or $self->info("Error checking what we selected") and return;

  	$self->do("update SITEQUEUES set ASSIGNED=ASSIGNED+1 where siteid=?",{bind_values=>[$siteid]});
  	$self->do("update SITEQUEUES set WAITING=WAITING-1 where siteid=?",{bind_values=>[$self->findSiteId("unassigned::site")]});
  	 
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
	
	$self->do("update QUEUEJDL set resultsJdl=null,path=null where queueid=?",{bind_values=>[$queueid]} );
	my $status='WAITING';
	my $data=$self->queryRow("select siteid,statusId,masterjob from QUEUE where queueid=?", undef, {bind_values=>[$queueid]})
	 or $self->info("Error getting the previous status of the job ") and return;
	 
	my $previousStatus=AliEn::Util::statusName($data->{statusId});
	my $previousSiteId= $data->{siteid};
	$self->info("UPDATING $previousStatus and $previousSiteId");
	
	my $unassignedId=$self->findSiteId("unassigned::site");

	$data->{masterjob}  and $status='INSERTING';
  $previousStatus =~ /^ERROR_I$/ and $status='INSERTING';
 	
	$self->do("UPDATE QUEUE SET statusId= ? ,resubmission= resubmission+1 ,started= '' ,
                 finished= '' ,exechostid= null,siteid=$unassignedId  WHERE queueid=? ",
		{bind_values=>[AliEn::Util::statusForML($status), $queueid]	} );
	$self->do("UPDATE SITEQUEUES set $previousStatus=$previousStatus-1 where siteid=?", {bind_values=>[$previousSiteId]});
	$self->do("UPDATE SITEQUEUES set $status=$status+1 where siteid=$unassignedId");
  
	#Should we delete the QUEUEPROC??? Nope, that's defined as on cascade delete
	
	#Finally, update the JOBAGENT
	
	my $done=$self->do("update JOBAGENT join QUEUE on (agentid=entryid) set counter=counter+1 where queueid=?",
	  {bind_values=>[$queueid]});
	if ($done =~ /0E0/){
		$self->info("The job agent is no longer there!!");
		my $info = $self->queryRow("select uncompress(origjdl) jdl , agentid from QUEUEJDL join QUEUE using (queueid) where queueid=?",
		                          undef, {bind_values=>[$queueid]});
		
		$info or $self->info("Error getting the jdl of the job") and return;
		my $jdl=$info->{jdl};
		$jdl =~ /(Requirements[^;]*);/ims
          or $self->info("Error getting the requirements from $jdl") and return;

      my $req = $1;
      $jdl =~ /(user\s*=\s*"([^"]*)")/si or $self->info("Error getting the user from '$jdl'") and next;
      $req.="; $1 ";
      my $params=$self->extractFieldsFromReq($req);
      $params->{entryId}= ($info->{agentid} || 0);
	  
	  $self->insert("JOBAGENT",$params);
	  my ($ret)=$self->getLastId("JOBAGENT");
	  $self->do("UPDATE QUEUE set agentId=$ret where queueId=$queueid");
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

  my ($data) = $self->getFieldsFromQueue($queueId, "statusId,exechostId, submithostId,siteId,agentid,userid,split");

  defined $data
    or $self->info( "In killProcess error during execution of database query")
    and return;

  %$data
    or $self->info("In killProcess process $queueId does not exist")
    and return;
 
  my $callerId=$self->findUserId($user);
  if (($data->{userid}!= $callerId) and ($user ne "admin")) {
    $self->info( "In killProcess process does not belong to '$user'");
    return;
  }

  $self->do("delete from QUEUE where queueid=?", {bind_values=>[$queueId]}) or return ;
  
  $self->do("update SITEQUEUES set $data->{statusId}=$data->{statusId}-1 where siteid=?", {bind_values=>[$data->{siteid}]});
  $self->insertJobMessage($queueId, "state", "Job has been killed");
  if ($data->{statusId} =~ /WAITING/){
  	$self->info("And reducing the number of agents");
  	$self->do("update JOBAGENT set counter=counter-1 where entryid=?", {bind_values=>[$data->{agentid}]})
  	
  }
  if ($data->{exechostId}) {
    my $exechost=$self->queryValue("SELECT host from QUEUE_HOST where hostid=?", undef, {bind_values=>[$data->{exechostid}]});
    $DEBUG and $self->debug(1, "Sending a signal to $exechost to kill the process... ");
    my $current = time() + 300;
    my ($ok) = $self->insertMessage(
      { TargetHost    => $exechost,
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
  if ($data->{split}) {
    $self->do("insert ignore into JOBSTOMERGE values (?)", {bind_values=>[$data->{split}]});
    $self->do("update ACTIONS set todo=1 where action='MERGING'");
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


sub checkPriorityValue() {
  my $self = shift;
  my $user = shift or $self->{LOGGER}->error("TaskQueue", "no username provided in checkPriorityValue");
  $self->debug(1, "Checking if the user $user exists");

  my $exists = $self->queryValue('select count(1) from PRIORITY join QUEUE_USER using (userid) where user=?', 
                     undef, {bind_values=>[$user]});
  if ($exists) {
    $self->debug(1, "$user entry for priority exists!");
  } else {
    $self->debug(1, "$user entry for priority does not exist!");
    my $set = {};
    $set->{'userid'}              = $self->getOrInsertFromLookupTable("user", $user);
    $set->{'priority'}            = "1.0";
    $set->{'maxparallelJobs'}     = 20;
    $set->{'nominalparallelJobs'} = 10;
    $set->{'computedpriority'}    = 1;

    #Job Quota
    $set->{'unfinishedJobsLast24h'}   = 0;
    $set->{'maxUnfinishedJobs'}       = 100;
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
    $self->insert("PRIORITY", $set);
  }
}

sub insertPriority {
  my $self = shift;
  $self->insert("PRIORITY", @_);
}

sub updatePriority {
  my $self = shift;
  $self->update("PRIORITY", @_);
}


sub updatePrioritySet {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("TaskQueue", "In updatePrioritySet user is missing")
    and return;
  my $set = shift;

  $self->debug(1, "In updatePrioritySet user is NOT missing");
  $self->update("PRIORITY join QUEUE_USER using (userid) ", $set, " user = ? ", {bind_values => [$user]});
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

  my $array = $self->query("SELECT 
 userid, unfinishedJobsLast24h, maxUnfinishedJobs, totalRunningTimeLast24h,
  maxTotalRunningTime, totalCpuCostLast24h, maxTotalCpuCost from PRIORITY join QUEUE_USER using (userid)
   where " . $self->reservedWord("user") . "=?", undef, {bind_values=>[$user]})
    or $self->info("Failed to getting data from PRIORITY table")
    and return (-1, "Failed to getting data from PRIORITY table");
  $array->[0]
    or $self->{LOGGER}->error("User $user does not exist")
    and return (-1, "User $user does not exist in PRIORITY table");

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
  return (1, $array->[0]->{userid});
}

sub insertNewPriorityUsers {
  my $self       = shift;

  return $self->do(
"INSERT INTO PRIORITY(userid, priority, maxparallelJobs, nominalparallelJobs, maxUnfinishedJobs, computedpriority, maxTotalCpuCost, 
  totalRunningTimeLast24h, waiting, unfinishedJobsLast24h, userload, running, totalCpuCostLast24h, maxTotalRunningTime) 
 SELECT userid, 1, 20, 10, 100, 100, 2000000, 0, 0, 0, 0, 0, 0, 1000000 from QUEUE_USER left join PRIORITY using (userid) where PRIORITY.userid is null"
  );
}
sub getPriorityUpdate {
  my $self       = shift;

  return $self->do("update PRIORITY p left join  
(select userid ,count(*) w from QUEUE where statusId=5 group by userid ) b using (userid)
 left join (select userid,count(*) r from QUEUE where statusId in (10,7,11) group by userid) b2 using (userid) 
 set waiting=coalesce(w,0), running=COALESCe(r,0) ,
userload=(running/maxparallelJobs), 
computedpriority=(if(running<maxparallelJobs, if((2-userload)*priority>0,50.0*(2-userload)*priority,1),1))");
} 


sub unfinishedJobs24PerUser {
  my $self = shift;
  return $self->do(
"update PRIORITY pr left join (select userid, count(1) as unfinishedJobsLast24h from QUEUE q where (statusId in (1,5,7,10,11,21)) and (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) group by userid) as C using (userid) set pr.unfinishedJobsLast24h=IFNULL(C.unfinishedJobsLast24h, 0)"
  ); # (status='INSERTING' or status='WAITING' or status='STARTED' or status='RUNNING' or status='SAVING' or status='OVER_WAITING')

}

sub cpuCost24PerUser {
  my $self = shift;
  return $self->do(
"update PRIORITY pr left join (select userid, sum(p.cost) as totalCpuCostLast24h , 
sum(p.runtimes) as totalRunningTimeLast24h  from QUEUE q 
join QUEUEPROC p using(queueId) 
where (unix_timestamp()>=q.received and unix_timestamp()-60*60*24<q.received ) group by userid) as C using (userid) set pr.totalRunningTimeLast24h=IFNULL(C.totalRunningTimeLast24h, 0), pr.totalCpuCostLast24h=IFNULL(C.totalCpuCostLast24h, 0)" );

}

sub changeOWtoW {
  my $self = shift;
  return $self->do(
"update QUEUE q join PRIORITY pr using (userid) set q.statusId=5 where (pr.totalRunningTimeLast24h<pr.maxTotalRunningTime and pr.totalCpuCostLast24h<pr.maxTotalCpuCost) and q.statusId=21" # WAITING - OVERWAITING
  );
}

sub changeWtoOW {
  my $self = shift;
  return $self->do(
"update QUEUE q join PRIORITY pr using (userid) set q.statusId=21 where (pr.totalRunningTimeLast24h>=pr.maxTotalRunningTime or pr.totalCpuCostLast24h>=pr.maxTotalCpuCost) and q.statusId=5" #OVERWAITING - WAITING
  );
}

sub updateFinalPrice {
  my $self     = shift;
  my $t        = shift;
  my $nominalP = shift;
  my $now      = shift;
  my $done     = shift;
  my $failed   = shift;
  my $update   = " UPDATE $t q, QUEUEPROC p SET finalPrice = round(p.si2k * $nominalP * price),chargeStatus=\'$now\'";

  my $where =
" WHERE (statusId=15 AND p.si2k>0 AND chargeStatus!=\'$done\' AND chargeStatus!=\'$failed\') and p.queueid=q.queueid"; # DONE
  my $updateStmt = $update . $where;
  return $self->do($updateStmt);

}

sub optimizerJobExpired {
  return
"( ( (statusId=15) || (statusId=-13) || (statusId=-12) || (statusId=-1) || (statusId=-2) || (statusId=-3) || (statusId=-4) || (statusId=-5) || (statusId=-7) || (statusId=-8) || (statusId=-9) || (statusId=-10) || (statusId=-11) || (statusId=-16) || (statusId=-17) || (statusId=-18) ) && ( received < (? - 7*86540) ) )";
#"( ( (status='DONE') || (status='FAILED') || (status='EXPIRED') || (status like 'ERROR%')  ) && ( received < (? - 7*86540) ) )";
}




#######
## optimizer Job/priority
#####

# WAITING RUNNNING STARTED SAVING


########
## optimizer Job/Expired
####

#sub getJobOptimizerExpiredQ1{
#  my $self = shift;
# return "where  (status in ('DONE','FAILED','EXPIRED') or status like 'ERROR%'  ) and ( mtime < addtime(now(), '-10 00:00:00')  and split=0) )";
#}

sub getJobOptimizerExpiredQ2 {
  my $self = shift;
  return
" left join QUEUE q2 on q.split=q2.queueid where q.split!=0 and q2.queueid is null and q.mtime<addtime(now(), '-10 00:00:00')";
}

sub getJobOptimizerExpiredQ3 {
  my $self = shift;
  return "where mtime < addtime(now(), '-10 00:00:00') and split=0";
}

########
### optimizer Job/Zombies
####

sub getJobOptimizerZombies {
  my $self   = shift;
  my $status = shift;
  return "q, QUEUEPROC p where $status and p.queueId=q.queueId and DATE_ADD(now(),INTERVAL -3600 SECOND)>lastupdate";
}

########
### optimizer Job/Charge
####

sub getJobOptimizerCharge {
  my $self           = shift;
  my $queueTable     = shift;
  my $nominalPrice   = shift;
  my $chargingNow    = shift;
  my $chargingDone   = shift;
  my $chargingFailed = shift;
  my $update =
" UPDATE $queueTable q, QUEUEPROC p SET finalPrice = round(p.si2k * $nominalPrice * price),chargeStatus=\'$chargingNow\'";
  my $where =
" WHERE (statusId=15 AND p.si2k>0 AND chargeStatus!=\'$chargingDone\' AND chargeStatus!=\'$chargingFailed\') and p.queueid=q.queueid";
  return $update . $where;
} # DONE







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

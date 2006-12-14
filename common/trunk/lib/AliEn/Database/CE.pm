#/**************************************************************************
# * Copyright(c) 2001-2002, ALICE Experiment at CERN, All rights reserved. *
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
package AliEn::Database::CE;

use AliEn::Database::TXT;
use strict;

use vars qw(@ISA);

@ISA=("AliEn::Database::TXT");

sub initialize{
  my $self=shift;
  $self->{DIRECTORY}="$self->{CONFIG}->{LOG_DIR}/CE.db";

  $self->{TABLES}->{JOBAGENT}="agentId varchar(40), 
                               batchId varchar(60),
                               workernode varchar(300), 
                               timestamp int, 
                               jobId int,
                               status varchar(15), jdl varchar(1000)";

  $self->{TABLES}->{MESSAGES}="jobId int,
                               procinfo varchar(200),
                               tag varchar(40),
                               timestamp int";

  $self->{TABLES}->{TOCLEANUP}="batchId varchar(60),
                                timestamp int";

  return $self->SUPER::initialize();
}


sub insertJobAgent{
  my $self=shift;
  my $data=shift;
  $data->{timestamp}=time;
  $data->{status}="QUEUED";
  return $self->insert("JOBAGENT", $data,@_);
}

sub updateJobAgent{
  my $self=shift;
  my $data=shift;
  $data->{timestamp}=time;
  $data->{status}="ACTIVE";
  $self->info("Updating the jobagent");
  my $done=$self->update("JOBAGENT", $data, @_);
  if ( $done =~ /^0E0$/){
    #Ok, the increment did not work. Let's insert the entry
    $self->info("Inserting a new jobagent");
    $done=$self->insert("JOBAGENT", $data);
  }

  return $done;
}

sub removeJobAgent {
   my $self = shift;
   my $needsCleanUp = shift;
   my $data = shift;
   my $key = (keys(%$data))[0]; #Use the first key! 
   $key or return;
   $data->{$key} or return;
   my $batchId = '';
   if ($needsCleanUp) {
     $self->debug(1,"This system needs JA cleanup");
     if ( $data->{batchId} ) {
       $self->debug(1,"Will cleanup JA with batchId=$data->{batchId}");
       $batchId = $data->{batchId};
     } else {
       $self->debug(1,"Will try to cleanup JA with $key=$data->{$key}");
       my $result = $self->query("SELECT batchId FROM JOBAGENT WHERE $key=$data->{$key}");
       $result = (@$result)[0]; # take the first and hopefully only one
       $result->{batchId} and $batchId = $result->{batchId};
       $self->debug(1,"batchId is $batchId");
     } 
     if ($batchId) {  
       $self->insert("TOCLEANUP",{ batchId   => $batchId, 
	 		           timestamp => time() }) if $batchId;
     } else {
       $self->info("No idea how to cleanup JobAgent with $key=$data->{$key}");
     }
   }
   $self->debug(1,"Will delete JA with $key=$data->{$key}");   
   $self->delete("JOBAGENT", "$key=\'$data->{$key}\'");
   return 1;
}

sub insertMessage {
  my $self=shift;
  my $jobId=shift;
  my $tag=shift; 
  my $message=shift;
  my $time=time;
  $self->lock("MESSAGES");
  my $done= $self->insert("MESSAGES", {jobId=>$jobId, procinfo=>$message,
			     tag=>$tag,  timestamp=>$time});
  $self->unlock("MESSAGES");
  $done or return;

  $self->updateJobAgent({jobId=>$jobId},"jobId='$jobId'");
}

sub retrieveMessages{
  my $self=shift;
  my $time=time;
  my $info=$self->query("SELECT * from MESSAGES where timestamp<$time");
  $self->delete("MESSAGES", "timestamp<$time");
  return $info;
  
}
##############################################################################
##############################################################################

1;

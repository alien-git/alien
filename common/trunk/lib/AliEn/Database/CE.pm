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

  $self->{TABLES}->{TOCLEANUP}="agentId varchar(40),
                                batchId varchar(60),
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
   my $data = shift;
   $self->insert("TOCLEANUP",{ batchId  => $data->{batchId}, 
			       timestamp=> time() }) if $data->{needsCleanUp};
   $self->delete("JOBAGENT", "batchId='$data->{batchId}'");
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

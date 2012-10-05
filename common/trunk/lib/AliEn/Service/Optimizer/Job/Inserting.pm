package AliEn::Service::Optimizer::Job::Inserting;

use strict;

use AliEn::Service::Optimizer::Job;

use vars qw(@ISA);
push(@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self   = shift;
  my $silent = shift;
  $self->{SLEEP_PERIOD} = 10;
  my $method = "info";
  $silent and $method = "debug";
  my @data;
  $silent and push @data, 1;
  $self->{INSERTING_COUNTING} or $self->{INSERTING_COUNTING} = 0;
  $self->{INSERTING_COUNTING}++;

  if ($self->{INSERTING_COUNTING} > 10) {
    $self->{INSERTING_COUNTING} = 0;
  } else {
    $method = "debug";
    @data   = (1);
  }
  $self->$method(@data, "The inserting optimizer starts");
  my $todo = $self->{DB}->queryValue("SELECT todo from ACTIONS where action='INSERTING'");
  $todo or return;
  $self->{DB}->update("ACTIONS", {todo => 0}, "action='INSERTING'");
  my $q = "1' and upper(origjdl) not like '\% SPLIT = \"\%";
  $self->{DB}->{DRIVER} =~ /Oracle/i and $q = "1 and REGEXP_REPLACE(upper(origjdl), '\\s*', '') not like '\%SPLIT=\"\%";
  my $done = $self->checkJobs($silent, $q, "updateInserting", 15, 15);

  $self->$method(@data, "The inserting optimizer finished");
  return;
}

sub updateInserting {
  my $self    = shift;
  my $queueid = shift;
  my $job_ca  = shift;

  my $status = "WAITING";

  $self->info("\n\nInserting a new job $queueid");

  my $user = $self->{DB}->queryValue("select user from QUEUE join QUEUE_USER using (userid) where queueid=?",
    undef, {bind_values => [$queueid]})
    or $self->info("Job $queueid doesn't exist")
    and return;

  my $set = {};
  eval {
    if (!$job_ca->isOK()) {
      die("incorrect JDL input");
    }

    my $done = $self->copyInput($queueid, $job_ca, $user)
      or die("error copying the input\n");

    my ($ok, $req) = $job_ca->evaluateExpression("Requirements");
    ($ok and $req)
      or die("error getting the requirements of the jdl");
    $self->debug(1, "Let's create the entry for the jobagent");
    $req =~ s{ \&\& \( other.LocalDiskSpace > \d+ \)}{}g;

    $done->{requirements} and $req .= " && $done->{requirements}";

    $ok = $job_ca->set_expression("Requirements", $req)
      or die("ERROR SETTING THE REQUIREMENTS TO $req");
    $set->{origjdl} = $job_ca->asJDL();

    ($ok, my $stage) = $job_ca->evaluateExpression("Prestage");
    if ($stage) {
      $self->putJobLog($queueid, "info", "The job asks for its data to be pre-staged");
      $status = "TO_STAGE";
      $req .= "  && other.TO_STAGE==1 ";
    }

    ($status) = $self->checkRequirements($req, $queueid, $status);

    if ($status ne "FAILED") {
      $req = $self->getJobAgentRequirements($req, $job_ca);
      $set->{agentId} = $self->{DB}->insertJobAgent($req)
        or die("error creating the jobagent entry\n");
    }
  };
  my $return = 1;
  if ($@) {
    $self->info("Error inserting the job: $@");
    $status = "ERROR_I";
    $self->{DB}->deleteJobToken($queueid);

    undef $return;
  }
  if (!$self->{DB}->updateStatus($queueid, "INSERTING", $status, $set)) {
    $self->{DB}->updateStatus($queueid, "INSERTING", "ERROR_I");
    $self->info("Error updating status for job $queueid");
    return;
  }
  $self->putJobLog($queueid, "state", "Job state transition from INSERTING to $status");

  $return and $self->debug(1, "Command $queueid inserted!");
  return $return

}

sub checkRequirements {
  my $self    = shift;
  my $tmpreq  = shift;
  my $queueid = shift;
  my $status  = shift;
  my $msg     = "";
  my $no_se   = {};

  while ($tmpreq =~ s/!member\(other.CloseSE,"([^:]*::[^:]*::[^:]*)"\)//si) {
    $no_se->{uc($1)} = 1;
  }
  my $ef_site = {};
  my $need_se = 0;
  while ($tmpreq =~ s/member\(other.CloseSE,"([^:]*::([^:]*)::[^:]*)"\)//si) {
    $need_se = 1;
    $no_se->{uc($1)} and $self->info("Ignoring the se $1 (because of !closese)") and next;
    $ef_site->{uc($2)} = {};
  }

  $need_se and !keys %$ef_site and $msg .= "conflict with SEs";
  if (!$msg) {
    my $no_ce = {};
    while ($tmpreq =~ s/!other.ce\s*==\s*"([^:]*::([^:]*)::[^:"]*)"//i) {
      my $cename = uc($1);
      my $site   = uc($2);
      $no_ce->{$cename} = 1;
      if ($need_se and $ef_site->{$site}) {
        $ef_site->{$site}->{$cename} = 1;
        $self->checkNumberCESite($site, $ef_site->{$site}) and next;
        $self->info("There are no more CE at the site");
        delete $ef_site->{$site};
      }
    }
    $need_se and !keys %$ef_site and $msg .= "conflict with SEs and !CE";

    my $ef_ce   = 0;
    my $need_ce = 0;
    while ($tmpreq =~ s/other.ce\s*==\s*"([^:]*::([^:]*)::[^:]*)"//i) {
      $need_ce = 1;
      $no_ce->{uc($1)} and $self->info("Ignoring the ce $1") and next;
      if ($need_se) {
        $ef_site->{uc($2)} or $self->info("This CE is not good for the sites that we have") and next;
      }
      $ef_ce = 1;
    }
    $need_ce and not $ef_ce and $msg .= "The CEs requested by the user cannot execute this job";

  }
  $msg
    and $status = "FAILED"
    and $self->putJobLog($queueid, "state", "Job going to FAILED, problem with requirements: $msg");
  return $status;
}

sub checkNumberCESite {
  my $self  = shift;
  my $site  = shift;
  my $no_ce = shift;
  my @bind  = ($site);

  my $query = "";
  foreach my $ce (keys %$no_ce) {
    $query .= "?,";
    push @bind, $ce;
  }
  $query =~ s/,$//;
  $self->info("Checking if there are any other available CES at the site $site (removing @bind)");
  return $self->{DB}
    ->queryValue("select count(1) from SITEQUEUES where site like concat('%\::',?,'::%') and site not in ($query)",
    undef, {bind_values => \@bind});
}

1

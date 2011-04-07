# JOB INFORMATION LOGGING
# AJP 06/2004
# ----------------------------------------------------------

package AliEn::JOBLOG;
use AliEn::Logger;
use strict;

sub new {
  my $proto = shift;
  my $self  = {};
  bless($self, (ref($proto) || $proto));

  $self->{LOGGER} = new AliEn::Logger;
  $self->{CONFIG} = AliEn::Config->new();

  if ($ENV{'ALIEN_JOBINFORMATION'}) {
    if (!-d $ENV{'ALIEN_JOBINFORMATION'}) {
      print STDERR "I don't write the JOBLOG for $ENV{'ALIEN_JOBINFORMATION'} \n";
      $self->{enabled} = 0;
    }
  } else {
    $self->{LOGGER}->info("JOBLOG",
      "WARNING!! The directory for the job log is not defined. Taking $self->{CONFIG}->{TMP_DIR}/joblog");
    $ENV{'ALIEN_JOBINFORMATION'} = "$self->{CONFIG}->{TMP_DIR}/joblog";
  }

  $self->{enabled} = 1;

  system("mkdir -p $ENV{'ALIEN_JOBINFORMATION'}");
  return $self;
}

sub setlogfile {
  my $self   = shift;
  my $jobid  = shift;
  my $jobdir = sprintf "%04d", int($jobid / 10000);
  $self->{JOBLOGFILE} = $ENV{'ALIEN_JOBINFORMATION'} . "/" . $jobdir . "/" . "$jobid.log";
  mkdir $ENV{'ALIEN_JOBINFORMATION'} . "/" . $jobdir, 0755;
}

sub putlog {
  my $self     = shift;
  my $jobid    = shift;
  my $tag      = shift;
  my $messages = join(" ", @_);
  my $now      = time;

  $self->{enabled} or return;

  $self->setlogfile($jobid);

  open OUTPUT, ">> $self->{JOBLOGFILE}";
  printf OUTPUT "$now [%-10s]: $messages\n", $tag;
  close OUTPUT;
}

sub getlog {
  my $self  = shift;
  my $jobid = shift;
  my @tags  = @_;

  if ($tags[0] eq "all") {
    undef @tags;
    @tags = ("proc", "error", "submit", "move", "state", "trace", "info");
  }

  grep (/^error$/, @tags) or push @tags, "error";

  $self->{enabled} or return;
  $self->setlogfile($jobid);
  map { $_ = "($_)" } @tags;
  my $status = join("|", @tags);
  open INPUT, "$self->{JOBLOGFILE}";
  my @result = grep (/\[($status)/, <INPUT>);
  close INPUT;
  $self->{LOGGER}->info("JOBLOG", "Looking for $status of $jobid and found $#result");

  return @result;
}

return 1;

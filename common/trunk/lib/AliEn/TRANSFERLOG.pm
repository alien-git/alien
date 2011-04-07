# TRANSFER INFORMATION LOGGING
# AJP 06/2004
# ----------------------------------------------------------

package AliEn::TRANSFERLOG;
use AliEn::Logger;

use AliEn::Database::Transfer;

use strict;
use vars qw($VERSION @ISA);

push @ISA, 'AliEn::Logger::LogObject';

sub new {
  my $proto = shift;
  my $self = shift || {};
  bless($self, (ref($proto) || $proto));
  $self->SUPER::new() or return;

  $self->{CONFIG} = AliEn::Config->new();

  if ($self->{DB}) {
    $self->info("Putting the messages in the database");
    $self->{enabled} = 1;
  } else {
    if ($ENV{'ALIEN_TRANSFERINFORMATION'}) {
      if (!-d $ENV{'ALIEN_TRANSFERINFORMATION'}) {
        print STDERR "I don't write the TRANSFERLOG for $ENV{'ALIEN_TRANSFERINFORMATION'} \n";
        $self->{enabled} = 0;
      }
    } else {
      $self->info(
        "WARNING!! The directory for the transfer log is not defined. Taking $self->{CONFIG}->{TMP_DIR}/transferlog");
      $ENV{'ALIEN_TRANSFERINFORMATION'} = "$self->{CONFIG}->{TMP_DIR}/transferlog";
    }

    $self->{enabled} = 1;

    system("mkdir -p $ENV{'ALIEN_TRANSFERINFORMATION'}");
  }
  return $self;
}

sub setlogfile {
  my $self        = shift;
  my $transferid  = shift;
  my $transferdir = sprintf "%04d", int($transferid / 10000);
  $self->{TRANSFERLOGFILE} = $ENV{'ALIEN_TRANSFERINFORMATION'} . "/" . $transferdir . "/" . "$transferid.log";
  mkdir $ENV{'ALIEN_TRANSFERINFORMATION'} . "/" . $transferdir, 0755;
}

sub putlog {
  my $self       = shift;
  my $transferid = shift;
  my $status     = shift;

  #     my $destination = shift;
  my $messages = join(" ", @_);

  #     my $now  = time;
  my $now = localtime;

  # 	$now =~ s/^\S+\s(.*):[^:]*$/$1/;

  $self->{enabled} or return;

  if ($self->{DB}) {
    $self->info("Putting the transferlog in the database");
    $self->{DB}->insertTransferMessage($transferid, $status, $messages);
    return 1;
  }
  $self->info("Putting the transferlog in the file");
  $self->setlogfile($transferid);

  open OUTPUT, ">> $self->{TRANSFERLOGFILE}";

  #     printf OUTPUT "$now [%-10s]: $messages\n", $status;
  printf OUTPUT "$now [%-10s]: $messages\n", $status;
  close OUTPUT;
}

sub getlog {
  my $self       = shift;
  my $transferid = shift;
  my @tags       = @_;

  @tags or push @tags, "STATUS", "INFO";

  grep (/^error$/, @tags) or push @tags, "error";

  if ($tags[0] eq "all") {
    undef @tags;

    #    push @tags,"error";
    #    push @tags,"STATUS";
  }

  $self->{enabled} or return;
  $self->setlogfile($transferid);
  map { $_ = "($_)" } @tags;
  my $status = join("|", @tags);
  open INPUT, "$self->{TRANSFERLOGFILE}";
  my @result = grep (/\[$status/i, <INPUT>);
  close INPUT;
  $self->info("Looking for $status of $transferid and found $#result");

  return @result;
}

return 1;

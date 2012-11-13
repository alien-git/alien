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

  system("mkdir -p $ENV{'ALIEN_JOBINFORMATION'}/agents/");
  return $self;
}
sub getListAgentSites {
   my $self=shift;
   my $year=shift;
   my $month=shift;
   my $day=shift;
   
   my $site=shift;
   
   my $dirName="$self->{CONFIG}->{TMP_DIR}/joblog/agents/$year/$month/$day/";
   $site and $dirName.=$site;
   
   
   opendir(my $dir, $dirName) or $self->{LOGGER}->info(1, "Error reading $dirName") and return;
   my @l = grep (! /^\.*$/, readdir($dir));
   closedir($dir);

   map {s/\.log//} @l;
   @l = grep (/./,@l); 
   $self->{LOGGER}->info(1, "AND GOT '@l' (from $dirName)");
   return sort @l;
 
}
sub setlogfile {
  my $self   = shift;
  my $jobid  = shift;
  my $jobdir = sprintf "%04d", int($jobid / 10000);
  $self->{JOBLOGFILE} = $ENV{'ALIEN_JOBINFORMATION'} . "/" . $jobdir . "/" . "$jobid.log";
  mkdir $ENV{'ALIEN_JOBINFORMATION'} . "/" . $jobdir, 0755;
}

sub setAgentfile {
  my $self = shift;
  my $info =shift;
  my ($ce, $id) = split(/\_/, $info,2);
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
   $year+=1900;
   $mon+=1;
  my $dir="/agents/$year/$mon/$mday/$ce";
  $self->{JOBLOGFILE} = $ENV{'ALIEN_JOBINFORMATION'} . "$dir/$id.log";
  my $tmp="";
  foreach my $part (split(/\//, $dir )){
     $tmp.="$part/";
     mkdir $ENV{'ALIEN_JOBINFORMATION'} . "$tmp" , 0755;
  }
  
 
}

sub putlog {
  my $self     = shift;
  my $jobid    = shift;
  my $tag      = shift;
  my $message = shift;
  my $time     = (shift || time);


  $self->{enabled} or return;
  if ($tag eq "agent"){
    $self->setAgentfile($jobid)
  } else {
    $self->setlogfile($jobid);
  }
  $message =~ s/\n//g;  
  open my $OUTPUT, ">>", "$self->{JOBLOGFILE}";
  printf $OUTPUT "$time [%-10s]: $message\n", $tag;
  close $OUTPUT;
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
  open my $INPUT, "<", "$self->{JOBLOGFILE}";
  my @result = sort(grep (/\[($status)/, <$INPUT>));
  
  close $INPUT;
  $self->{LOGGER}->info("JOBLOG", "Looking for $status of $jobid and found $#result");

  return @result;
}
sub getAgentLog {
  my $self=shift;
  my $year=shift;
  my $month=shift;
  my $day=shift;
  my $site=shift;
  my $id=shift;
  my $fileName="$ENV{ALIEN_JOBINFORMATION}/agents/$year/$month/$day/$site/$id.log";
  $self->{LOGGER}->info("JOBLOG", "Reading $fileName");
  open (my $file, "<", $fileName) 
   or $self->{LOGGER}->info("JOBLOG", "Error reading '$fileName'") and  return;
  my @content= <$file>;
  close $file;
  map { s/^(\d+)// and $_=localtime($1).$_} @content;
  
  return @content;
 
}
return 1;

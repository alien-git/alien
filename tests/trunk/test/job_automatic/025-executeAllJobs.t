use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_JOBAGENT_RETRY}=1;
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_automatic/008-split") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  print "Executing all the jobs waiting in the system";
  my $admincat=AliEn::UI::Catalogue::LCM::Computer->new({"user","$ENV{'USER'}","role","admin"});
  $admincat or exit (-1);

  my (@jobs)=$cat->execute("top", "-all")  or print "There are no jobs at all...\n" and  exit(0);
  print "There are some jobs to execute\n";
  my $waitingJobs = 0;
  foreach my $job (@jobs) {
     defined($job) and $job->{status} =~ /^WAITING$/ and $waitingJobs++;
  }
  my $i=$waitingJobs;

  while($i>0){
    $admincat->execute("queue", "open $cat->{CONFIG}->{ORG_NAME}::CERN::testCE") 
      or print "Error opening the queue\n" and exit(-2);

    $cat->execute("request") or print "Error requesting a job\n" and exit(-2);
    print "We have executed all the jobs!!\n";
    my @jobs=$cat->execute("top", "-status WAITING -status INSERTING -status RUNNING -status SAVING -status SAVED");
    @jobs or last;
    print "There are still some jobs waiting. Sleeping 10 seconds and retrying";
    sleep(15);
    $i--;

  }

  print "All right, seems like all jobs are in a ready state. Let's do a final checkup...\n";

  my $notok=0;
  print "\n";
  print "Getting top -all information from the Catalogue:\n";
  (@jobs)=$cat->execute("top", "-all")  or exit(-2);
  foreach my $job (@jobs) {
        ($job->{status} =~ /^(INSERTED)|(WAITING)|(ASSIGNED)|(STARTED)|(RUNNING)|(SAVING)|(SAVED)$/)
          and print "ATTENTION TO JOB: $job->{queueId} was just now in status: $job->{status}\n"
          and $notok=1;
  }

  (@jobs)=$cat->execute("top", "-status SPLIT");
  @jobs  and print "one job is still split... let's wait for the merging" and sleep(60);
  $cat->close();
  $notok and exit(-2);
  print "DONE!!\n";
  print "ok\n";
}

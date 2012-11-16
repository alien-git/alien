use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_JOBAGENT_RETRY}=1;
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_automatic/008-split") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  print "Executing all the jobs waiting in the system";
  my $admincat=AliEn::UI::Catalogue::LCM::Computer->new({"user","$ENV{'USER'}","role","admin"});
  $admincat or exit (-1);

  my $i=35;

  while($i>0){
    $admincat->execute("queue", "open $cat->{CONFIG}->{ORG_NAME}::CERN::testCE") 
      or print "Error opening the queue\n" and exit(-2);

    $cat->execute("request");
    print "We have executed all the jobs!!\n";
    my @jobs=$cat->execute("top", "-status ASSIGNED -status WAITING -status INSERTING -status RUNNING -status SAVING -status SAVED -status SPLIT");
    @jobs or last;
    print "There are still some jobs waiting. Sleeping 10 seconds and retrying. We can still try $i times\n";
    sleep(10);
    $i--;

  }

  print "All right, seems like all jobs are in a ready state. Let's do a final checkup...\n";

  my $notok=0;
  print "\n";
  print "Getting top -all information from the Catalogue:\n";
  my @jobs=$cat->execute("top");
  print "Top -all worked\n";
  my $split=0;
  foreach my $job (@jobs) {
    print "Checking the job $job->{queueId}\n";
        ($job->{status} =~ /^(INSERTED)|(WAITING)|(ASSIGNED)|(STARTED)|(RUNNING)|(SAVING)|(SAVED)$/)
          and print "ATTENTION TO JOB: $job->{queueId} was just now in status: $job->{status}\n"
          and $notok=1;
    $job->{status}=~ /^SPLIT$/ and $split=1;
  }
  print "And now for the split\n";
  $cat->close();
  $split and print "one job is still split... let's wait for the merging" and sleep(60);
  
  $notok and exit(-2);
  print "DONE!!\n";
  print "ok\n";
}

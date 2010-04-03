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
  my $i=10;
  while($i>0){
    $admincat->execute("queue", "open $cat->{CONFIG}->{ORG_NAME}::CERN::testCE") 
      or print "Error opening the queue\n" and exit(-2);

    my @jobs=$cat->execute("top", "-status", "WAITING","-status", "INSERTING");
    @jobs or last;  
    $cat->execute("request") or print "Error requesting a job\n" and exit(-2);
    print "We have executed all the jobs!!\n";
    $i--;

  }

  my $stillToWait=0;
  my $timeItOut=0;
  while ($timeItOut < 20) {

    ($timeItOut eq 10) and  ($cat->execute("request") and print "Did again a request, maybe this helps!!\n" or print "Error requesting a job\n" and exit(-2));
    
    print "\n";
    print "Getting top -all information from the Catalogue:\n";
    my (@jobs)=$cat->execute("top", "-all")  or exit(-2);
    foreach my $job (@jobs) {
      $stillToWait and last;
      ($job->{status} =~ /^(INSERTED)|(WAITING)|(ASSIGNED)|(STARTED)|(RUNNING)|(SAVING)|(SAVED)$/)
         and $stillToWait=1 and print "matched job in status: ($job->{status})";
    }
    print "\n";
    my $waited = $timeItOut*60;
    print "We already waited: $waited seconds\n";
    $stillToWait or last;
    print "There are still jobs we need to wait for. Sleeping 60 seconds ...\n";
    sleep(60);
    $stillToWait=0;
    $timeItOut = $timeItOut + 1;
  }

  print "All right, seems like all jobs are in a ready state. Let's do a final checkup...\n";

  my $notok=0;
  print "\n";
  print "Getting top -all information from the Catalogue:\n";
  my (@jobs)=$cat->execute("top", "-all")  or exit(-2);
  foreach my $job (@jobs) {
        ($job->{status} =~ /^(INSERTED)|(WAITING)|(ASSIGNED)|(STARTED)|(RUNNING)|(SAVING)|(SAVED)$/)
          and print "ATTENTION TO JOB: $job->{queueId} was just now in status: $job->{status}\n"
          and $notok=1;
  }

  $notok and exit(-2);
  print "DONE!!\n";
  $cat->close();
  print "ok\n";
}

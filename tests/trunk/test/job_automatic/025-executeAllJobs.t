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

  my $notok=0;

  my (@info)=$cat->execute("top", "-all")  or exit(-2);

  foreach (@info) {
     if ( $_->{status} ne "DONE" ) {
        print "ATTENTION TO JOB: $_->{queueId} was just now in status: $_->{status}\n";
        $notok=1;
     }
  }
  $notok and exit(-2);


  print "DONE!!\n";
  $cat->close();
  print "ok\n";
}

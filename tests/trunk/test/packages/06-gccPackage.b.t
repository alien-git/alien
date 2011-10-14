#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);
#use AliEn::Service::PackMan; # needed for includeTest 76
use AliEn::PackMan; # needed for includeTest 76
use Cwd;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);

  my $id=shift or print "Error getting the job id\n" and exit(-2);
  my $procDir=checkOutput($cat, $id) or exit(-2);

  print "Job executed successfully!!!\n";
  my $installdir="$ENV{ALIEN_HOME}/packages/newuser/sourcePackage/1.0/";
  print "Checking if the directory exist\n";
  (-d $installdir) 
    or print "Error the directory $installdir doesn't exist\n" and exit(-2);
  (-f "$installdir/my_compiled") or 
    print "The file 'my_compiled is not in the installation directory\n"
      and exit(-2);

  my ($output)=$cat->execute("get", "$procDir/stdout") or exit(-2);

  open (my $FILE, "<", $output) or print "Error checking the output of the job"
    and exit(-2);
  grep (/YUHUUU/, <$FILE>) or print "The command didn't say YUHUUU\n" and exit(-2);
  close $FILE;
  $cat->close();


  ok(1);
}

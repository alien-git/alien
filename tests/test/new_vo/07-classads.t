#!/bin/env alien-perl

BEGIN {
  unless(grep /blib/, @INC) {
    chdir 't' if -d 't';
    unshift @INC, '../lib' if -d '../lib';
  }
}

use strict;
use Test;

BEGIN { plan tests => 1 }


{
print "'use AliEn::JDL ...' test(s)...\n";

use AliEn::JDL;

my $host = Net::Domain::hostfqdn();
my $d =
   AliEn::Database::TaskQueue->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "processes", "ROLE", "admin", })
  or print "Error connecting to the database\n" and exit(-2);

my $host_jdl='Requirements = ( other.Type == "Job" ); GridPartitions = {"Validation"}; Type = "machine";';
 
my  $job_jdl='Requirements= Member(other.GridPartitions, "Validation");      Type="Job";';

my $job2_jdl='Requirements= Member(other.GridPartitions, "OtherValidation"); Type="Job";';

print "Creating the JDLs...";
my $job_ca = AliEn::JDL->new($job_jdl);
if ( !$job_ca or !$job_ca->isOK() ) {
  print  "Got an incorrect job ca";
  exit -1;
}

my $job2_ca = AliEn::JDL->new($job2_jdl);
if ( !$job2_ca or !$job2_ca->isOK() ) {
  print  "Got an incorrect job ca";
  exit -1;
}

my $host_ca = AliEn::JDL->new($host_jdl);
if ( !$host_ca or !$host_ca->isOK() ) {
  print  "Got an incorrect host ca";
  exit -1;
}

print "ok\nChecking that there is a match...";
my ($match,$rank) = $job_ca->Match($host_ca,$d);
if ( !$match ) {
  print "Error there is no match!!\n";
  exit (-1);
}
print "ok\nChecking that there is no match...";
 ($match,$rank) = $job2_ca->Match($host_ca,$d);
if ( $match ) {
  print "Error there is a match!!\n";
  exit (-1);

}

ok(1);

}

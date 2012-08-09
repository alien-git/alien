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
print "'use Classad ...' test(s)...\n";

use AlienClassad;

my $host_jdl='
[ 
   Requirements = ( other.Type == "Job" ); 
   GridPartition =  {"Validation"}  ; 
   Type = "machine";
]' ;
 
my  $job_jdl=' 
[
   Requirements= Member(other.GridPartition, "Validation");
   Type="Job";
]';

my $job2_jdl='
[
   Requirements= Member(other.GridPartition, "OtherValidation");
   Type="Job";
]';

print "Creating the classads...";
my $job_ca = AlienClassad::AlienClassad->new($job_jdl);
if ( ! $job_ca->isOK() ) {
  print  "Got an incorrect job ca";
  exit -1;
}

my $job2_ca = AlienClassad::AlienClassad->new($job2_jdl);
if ( ! $job2_ca->isOK() ) {
  print  "Got an incorrect job ca";
  exit -1;
}

my $host_ca = AlienClassad::AlienClassad->new($host_jdl);
if ( ! $host_ca->isOK() ) {
  print  "Got an incorrect host ca";
  exit -1;
}

print "ok\nChecking that there is a match...";
my ($match,$rank) = AlienClassad::Match($job_ca,$host_ca);
if ( ! $match ) {
  print "Error there is no match!!\n";
  exit (-1);

}
print "ok\nChecking that there is no match...";
 ($match,$rank) = AlienClassad::Match($job2_ca,$host_ca);
if ( $match ) {
  print "Error there is a match!!\n";
  exit (-1);

}

ok(1);

}

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

{ # check 'use ...'
  ok($@ =~ //);

  my $jobs=`find $ENV{ALIEN_ROOT}/lib/perl5/site_perl/*/i*/AliEn $ENV{ALIEN_ROOT}/lib/perl5/site_perl/*/AliEn -name "*.pm"`;

  my @modules=split("\n", $jobs);
  my $path="$ENV{ALIEN_ROOT}/lib/perl5/site_perl/";
  map { s/^$path[\d\.]*(\/[^\/]*)?\/AliEn\///} @modules;
  map { s/\.pm$//} @modules;
  map { s/\//::/g} @modules;
  
  @modules=grep (!/ProxyServer/, @modules);
  @modules=grep (!/Portal/, @modules);
  @modules=grep (!/Service::API/, @modules);
  
  #	print "Got @modules\n";
  #exit;
  foreach my $module ("Database", "Catalogue", "Service", "LCM", @modules) {
    print "Checking $module...";
    eval "require AliEn::$module" 
      or print "Error requiring the package\n $! $@\n" and exit(-2);
    print "\tok\n";
  }
  my @databases=grep (/^Database$/, @modules);

  print "Checking how many installations there are ( $#databases+1)\n";
  if ($#databases >0) {
    print "THERE ARE TOO MANY DATABASE MODULES!!! @databases\n";
    exit(-2);
  }


}

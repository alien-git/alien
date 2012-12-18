#!/usr/bin/perl
use strict;
use AliEn::Config;

my $config = new AliEn::Config( { "SILENT", 1 } );

$config or print STDERR "ERROR: getting the configuration\n" and exit;
my $var = shift;

($var)
  or print STDERR "ERROR: no variable specified\nUsage alien -x $0 <varName>\n"
  and exit;

print "$config->{$var}";
exit;

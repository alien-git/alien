#!/usr/bin/perl -w

#use AliEn::Alice::Commands::Analysis;

use strict;

my $command=(shift or "");


print "COMMAND $command\n";

eval "require $command" or print STDERR "Error requiring the package $command\n$@\nDoes the command  exist?\n" and exit;


print "Validating the output (@ARGV)...\n";
my $c=$command->new() or exit;

$c->Validate(@ARGV);

print "VALIDATION DONE!!!!\n";
exit;

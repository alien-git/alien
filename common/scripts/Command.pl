#!/usr/bin/perl -w
###############################################################################
# Command to test writing a file to the mss
###############################################################################
###############################################################################

use strict;

use Getopt::Long ();

{
  my $commandName=(shift or "");
  print "Executing the AliEn Command '$commandName'\n";
  $commandName="AliEn::$commandName";

  my $command;
  eval "require $commandName"
    or  print "ERROR requiring the command $commandName. Does it exit?\n$@\n" and
    exit -1;
  print "Arguments: @ARGV\n";
  $command = new $commandName({@ARGV}); 
  $command or 
    print "ERROR creating an instance of $command" and  exit -1;
  

###############################################################################
    # Configure and run an event with AliRoot
###############################################################################
    $command->Initialize(@ARGV) or print STDERR "Error initializing $!\n" and exit -1;
    print "Configuration done!!\n\n";
    $command->Execute(@ARGV);
}


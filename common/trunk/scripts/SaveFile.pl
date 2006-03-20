#!/usr/bin/perl -w
###############################################################################
# Command to test writing a file to the mss
###############################################################################
###############################################################################

use AliEn::Command::SaveFile;
use strict;

use Getopt::Long ();

{
    print "This script tests writing a file in the mss\n";
    my $command = new AliEn::Command::SaveFile();

###############################################################################
    # Configure and run an event with AliRoot
###############################################################################
    $command->Initialize() or print STDERR "Error initializing $!\n" and exit;
    print "Configuration done!!\n\n";
    $command->Execute();
}

